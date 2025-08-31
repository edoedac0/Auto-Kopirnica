import os
import io
import json
import shutil
import socket
import threading
import time
import subprocess
from datetime import datetime, timezone

from flask import Flask, request, render_template, redirect, url_for, send_file, jsonify, abort
from werkzeug.utils import secure_filename

# Third-party deps
import qrcode
from PyPDF2 import PdfReader
from PyPDF2 import PdfWriter
from PIL import Image

try:
    # Optional but recommended for silent printing on Windows
    import win32api  # type: ignore
    import win32print  # type: ignore
except Exception:
    win32api = None
    win32print = None


# ----------------------------
# Configuration
# ----------------------------
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# Load .env if present to pick up PUBLIC_BASE_URL etc.
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv(os.path.join(BASE_DIR, '.env'))
except Exception:
    pass
JOBS_DIR = os.path.join(BASE_DIR, 'jobs')
os.makedirs(JOBS_DIR, exist_ok=True)

PORT = int(os.environ.get('PORT', 5000))
HOST = os.environ.get('HOST', '0.0.0.0')

ALLOWED_EXTENSIONS = {
    'pdf', 'png', 'jpg', 'jpeg', 'bmp', 'gif', 'tif', 'tiff',
    'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'txt'
}


def allowed_file(filename: str) -> bool:
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def get_local_ip() -> str:
    """Best-effort to determine a LAN IP suitable for QR/link."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.3)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        try:
            return socket.gethostbyname(socket.gethostname())
        except Exception:
            return '127.0.0.1'


def now_utc() -> datetime:
    """Timezone-aware current UTC datetime (no deprecation warnings)."""
    return datetime.now(timezone.utc)


def generate_job_id() -> str:
    return now_utc().strftime('%Y%m%d%H%M%S%f')


def count_pdf_pages(path: str) -> int:
    try:
        with open(path, 'rb') as f:
            reader = PdfReader(f)
            return len(reader.pages)
    except Exception:
        return 1


def ext_of(path: str) -> str:
    return os.path.splitext(path)[1].lower().lstrip('.')


# ----------------------------
# In-memory job store
# ----------------------------
# jobs[job_id] = {
#   'id': job_id,
#   'files': [ {'filename': original, 'path': abs_path, 'pages': int, 'ext': str} ],
#   'options': { 'paper_size': 'A4', 'color': 'BW', 'duplex': 'No', 'copies': 1 },
#   'price': 0.0,
#   'stage': 'upload' | 'ready' | 'assigned' | 'printing' | 'done' | 'deleted',
#   'created_at': iso-str,
#   'assigned': False,
#   'total_pages': int
# }

jobs = {}
jobs_lock = threading.Lock()


def add_files_to_job(job: dict, saved_files: list):
    """Add uploaded files to a job, computing pages where possible."""
    for saved_path, original_name in saved_files:
        ext = ext_of(saved_path)
        pages = 1
        if ext == 'pdf':
            pages = count_pdf_pages(saved_path)
        else:
            # Non-PDF: treat each file as 1 page for pricing estimate.
            pages = 1

        job['files'].append({
            'filename': original_name,
            'path': saved_path,
            'pages': pages,
            'ext': ext,
            'selected_pages': None,
            'selected_text': ''
        })

    # Update totals
    job['total_pages'] = sum(f['pages'] for f in job['files'])
    job['stage'] = 'ready' if job['files'] else 'upload'


def ensure_job(job_id: str) -> dict:
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            abort(404, description='Job not found')
        # Bump last activity timestamp on any access
        job['last_activity'] = time.time()
        return job


def calc_price(job: dict, options: dict) -> float:
    """Calculate price based on per-page rules.

    Rules:
      - A4 BW 0.10/page, A4 Color 0.30/page
      - A3 doubles per-page price
      - Multiply by copies
    """
    # Account for custom per-file page selection when present
    pages = 0
    for f in job.get('files', []):
        sel = f.get('selected_pages')
        if sel:
            pages += len(sel)
        else:
            pages += int(f.get('pages', 0))
    paper_size = options.get('paper_size', 'A4')
    color = options.get('color', 'BW')
    copies = int(options.get('copies', 1)) or 1

    base = 0.10 if color == 'BW' else 0.30
    if paper_size == 'A3':
        base *= 2.0

    return round(pages * base * copies, 2)


def parse_page_ranges(spec: str, total_pages: int) -> list[int]:
    """Parse a string like "2-6, 9, 12-16" into sorted unique 1-based pages.

    - Invalid tokens are ignored.
    - Pages are clamped to [1, total_pages].
    - Duplicates removed; result sorted ascending.
    """
    pages: set[int] = set()
    if not spec:
        return []
    raw = spec.replace(';', ',').replace(' ', '')
    for token in raw.split(','):
        if not token:
            continue
        if '-' in token:
            try:
                a_str, b_str = token.split('-', 1)
                a = int(a_str)
                b = int(b_str)
                if a > b:
                    a, b = b, a
                a = max(1, a)
                b = min(total_pages, b)
                for p in range(a, b + 1):
                    if 1 <= p <= total_pages:
                        pages.add(p)
            except Exception:
                continue
        else:
            try:
                p = int(token)
                if 1 <= p <= total_pages:
                    pages.add(p)
            except Exception:
                continue
    return sorted(pages)


def subset_pdf(src_pdf: str, pages: list[int], out_pdf: str | None = None) -> str | None:
    """Create a new PDF containing only 1-based pages.

    Returns the output PDF path, or None on failure.
    """
    if not pages:
        return None
    try:
        with open(src_pdf, 'rb') as f:
            reader = PdfReader(f)
            writer = PdfWriter()
            for p in pages:
                idx = p - 1
                if 0 <= idx < len(reader.pages):
                    writer.add_page(reader.pages[idx])
            if writer.get_num_pages() == 0:
                return None
            if out_pdf is None:
                root, _ = os.path.splitext(src_pdf)
                out_pdf = f"{root}_subset.pdf"
            with open(out_pdf, 'wb') as out:
                writer.write(out)
            return out_pdf
    except Exception:
        return None


def try_find_sumatra() -> str | None:
    """Try to locate SumatraPDF executable for silent printing of PDFs."""
    candidates = [
        os.path.join(BASE_DIR, 'bin', 'SumatraPDF.exe'),
        r"C:\\Program Files\\SumatraPDF\\SumatraPDF.exe",
        r"C:\\Program Files (x86)\\SumatraPDF\\SumatraPDF.exe",
    ]
    for c in candidates:
        if os.path.isfile(c):
            return c
    # Try on PATH
    for pathdir in os.environ.get('PATH', '').split(os.pathsep):
        exe = os.path.join(pathdir, 'SumatraPDF.exe')
        if os.path.isfile(exe):
            return exe
    return None


def try_find_soffice() -> str | None:
    """Try to locate LibreOffice soffice.exe for headless conversions."""
    candidates = [
        os.path.join(BASE_DIR, 'bin', 'soffice.exe'),
        r"C:\\Program Files\\LibreOffice\\program\\soffice.exe",
        r"C:\\Program Files (x86)\\LibreOffice\\program\\soffice.exe",
    ]
    for c in candidates:
        if os.path.isfile(c):
            return c
    for pathdir in os.environ.get('PATH', '').split(os.pathsep):
        exe = os.path.join(pathdir, 'soffice.exe')
        if os.path.isfile(exe):
            return exe
    return None


def image_to_pdf(src_path: str) -> str | None:
    """Convert a single image to a 1-page PDF using Pillow."""
    try:
        im = Image.open(src_path)
        # Ensure RGB for PDF
        if im.mode in ("RGBA", "P"):
            im = im.convert("RGB")
        out = os.path.splitext(src_path)[0] + "_converted.pdf"
        im.save(out, "PDF", resolution=300.0)
        return out
    except Exception:
        return None


def office_to_pdf(src_path: str) -> str | None:
    """Convert Office/TXT documents to PDF using LibreOffice in headless mode."""
    soffice = try_find_soffice()
    if not soffice:
        return None
    try:
        outdir = os.path.dirname(src_path)
        # soffice --headless --convert-to pdf --outdir <dir> <file>
        subprocess.run([soffice, "--headless", "--convert-to", "pdf", "--outdir", outdir, src_path],
                       check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        candidate = os.path.splitext(src_path)[0] + ".pdf"
        if os.path.isfile(candidate):
            return candidate
    except Exception:
        pass
    return None


def ensure_pdf(src_path: str, ext: str) -> str | None:
    """Return a PDF path for the source file, converting when possible."""
    ext = ext.lower()
    if ext == 'pdf':
        return src_path
    image_ext = { 'png','jpg','jpeg','bmp','gif','tif','tiff','webp' }
    office_ext = { 'doc','docx','xls','xlsx','ppt','pptx','txt' }
    if ext in image_ext:
        return image_to_pdf(src_path)
    if ext in office_ext:
        return office_to_pdf(src_path)
    return None


def print_pdf_silent(path: str, copies: int = 1) -> bool:
    """Attempt to silently print a PDF. Returns True if we dispatched.

    Preference order:
      1) SumatraPDF.exe -print-to-default -silent
      2) Adobe Reader AcroRd32.exe /t
      3) ShellExecute print (may show dialog)
    """
    # 1) Sumatra
    sumatra = try_find_sumatra()
    if sumatra:
        try:
            # Sumatra supports specifying copies in -print-settings
            settings = f"copies={copies}" if copies > 1 else None
            cmd = [sumatra, '-print-to-default', '-silent']
            if settings:
                cmd += ['-print-settings', settings]
            cmd.append(path)
            subprocess.run(cmd, check=False)
            return True
        except Exception:
            pass

    # 2) Adobe Reader (will generally be silent, but may flash a process)
    acro_paths = [
        r"C:\\Program Files\\Adobe\\Acrobat Reader DC\\Reader\\AcroRd32.exe",
        r"C:\\Program Files (x86)\\Adobe\\Acrobat Reader DC\\Reader\\AcroRd32.exe",
    ]
    for acro in acro_paths:
        if os.path.isfile(acro):
            try:
                # /t <file> "<printer>"
                printer = None
                if win32print:
                    try:
                        printer = win32print.GetDefaultPrinter()
                    except Exception:
                        printer = None
                args = [acro, '/t', path]
                if printer:
                    args += [printer]
                # No native copies option; repeat if >1
                for _ in range(max(1, copies)):
                    subprocess.run(args, check=False)
                return True
            except Exception:
                pass

    # 3) ShellExecute print (fallback, may show UI, repeats for copies)
    try:
        if hasattr(os, 'startfile'):
            for _ in range(max(1, copies)):
                os.startfile(path, 'print')  # type: ignore[attr-defined]
            return True
        if win32api:
            for _ in range(max(1, copies)):
                win32api.ShellExecute(0, 'print', path, None, '.', 0)
            return True
    except Exception:
        pass
    return False


def print_non_pdf(path: str, copies: int = 1) -> bool:
    """Fallback printing for non-PDF via ShellExecute/system association."""
    try:
        if hasattr(os, 'startfile'):
            for _ in range(max(1, copies)):
                os.startfile(path, 'print')  # type: ignore[attr-defined]
            return True
        if win32api:
            for _ in range(max(1, copies)):
                win32api.ShellExecute(0, 'print', path, None, '.', 0)
            return True
    except Exception:
        pass
    return False


def print_job(job_id: str):
    """Worker thread: print files for a job and clean up."""
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            return
        job['stage'] = 'printing'
    print(f"[print_job {job_id}] printing {len(job.get('files', []))} files with options {job.get('options')}")

    options = job.get('options', {})
    copies = int(options.get('copies', 1)) or 1

    # Attempt to print each file
    for f in job.get('files', []):
        path = f['path']
        try:
            pdf_path = ensure_pdf(path, f['ext'])
            # Apply custom page selection if available (only for PDFs)
            pages_sel = f.get('selected_pages') or []
            if pdf_path:
                use_path = pdf_path
                if pages_sel:
                    subset_path = subset_pdf(pdf_path, pages_sel)
                    if subset_path:
                        use_path = subset_path
                printed = print_pdf_silent(use_path, copies=copies)
                if not printed and pdf_path != path:
                    # Fallback to generic print if Sumatra not found
                    printed = print_non_pdf(use_path, copies=copies)
            else:
                # Unknown type; last resort may show UI
                printed = print_non_pdf(path, copies=copies)
            # Small delay between dispatches to avoid spooling collision
            time.sleep(0.5)
        except Exception:
            printed = False
        print(f"[print_job {job_id}] dispatched {os.path.basename(path)} -> printed={printed}")

    with jobs_lock:
        # Mark as done and schedule deletion
        job['stage'] = 'done'
    print(f"[print_job {job_id}] done; scheduling cleanup")

    # Allow a short grace period so the Done page can load
    time.sleep(2)

    # Delete files and job folder
    try:
        job_folder = os.path.join(JOBS_DIR, job_id)
        if os.path.isdir(job_folder):
            shutil.rmtree(job_folder, ignore_errors=True)
    except Exception:
        pass

    with jobs_lock:
        job['stage'] = 'deleted'
    print(f"[print_job {job_id}] cleaned up and marked deleted")


def delete_job_files(job_id: str):
    """Remove job folder and files from disk."""
    try:
        job_folder = os.path.join(JOBS_DIR, job_id)
        if os.path.isdir(job_folder):
            shutil.rmtree(job_folder, ignore_errors=True)
    except Exception:
        pass

def cleanup_expired_jobs(timeout: int = 60, interval: int = 5) -> None:
    """Background worker that deletes jobs inactive for over `timeout` seconds."""
    while True:
        time.sleep(interval)
        now = time.time()
        expired: list[str] = []
        with jobs_lock:
            for jid, job in list(jobs.items()):
                if job.get('stage') in ('printing', 'done', 'deleted', 'assigned'):
                    continue
                last = job.get('last_activity', now)
                if now - last >= timeout:
                    job['stage'] = 'deleted'
                    expired.append(jid)
        for jid in expired:
            delete_job_files(jid)
            with jobs_lock:
                jobs.pop(jid, None)


cleanup_thread = threading.Thread(target=cleanup_expired_jobs, daemon=True)
cleanup_thread.start()


# ----------------------------
# Flask app
# ----------------------------
app = Flask(__name__)


@app.route('/')
def index():
    # Build upload URL for QR/landing
    public_base = os.environ.get('PUBLIC_BASE_URL')
    if public_base:
        base = public_base.rstrip('/')
    else:
        # Fallback to LAN IP for local kiosk
        base = f"http://{get_local_ip()}:{PORT}"
    upload_url = f"{base}/upload"

    # Generate QR as inline PNG
    qr = qrcode.QRCode(box_size=10, border=2)
    qr.add_data(upload_url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format='PNG')
    qr_png = buf.getvalue()
    qr_b64 = 'data:image/png;base64,' + (buf.getvalue()).hex()  # placeholder, will convert properly below

    # Proper base64
    import base64
    qr_b64 = 'data:image/png;base64,' + base64.b64encode(qr_png).decode('ascii')

    return render_template('index.html', upload_url=upload_url, qr_b64=qr_b64)


@app.route('/check_job')
def check_job():
    """Return the next ready, unassigned job for kiosk to handle.

    Response JSON:
      { status: 'none' } or { status: 'ok', job_id: '...' }
    """
    with jobs_lock:
        # Find earliest ready job not yet assigned
        ready = [j for j in jobs.values() if j['stage'] == 'ready' and not j.get('assigned')]
        ready.sort(key=lambda j: j['created_at'])
        if not ready:
            return jsonify({'status': 'none'})
        job = ready[0]
        job['assigned'] = True
        job['stage'] = 'assigned'
        return jsonify({'status': 'ok', 'job_id': job['id']})


@app.route('/upload', methods=['GET', 'POST'])
def upload_new():
    """Mobile upload page for creating a new job on first file upload."""
    if request.method == 'POST':
        files = request.files.getlist('files')
        if not files:
            return render_template('upload.html', job=None, error='Odaberite barem jednu datoteku.')

        # Save files, creating job only after we have the first file saved
        job_id = None
        saved = []
        job_folder = None
        for f in files:
            if f and allowed_file(f.filename):
                original_name = secure_filename(f.filename)
                # Lazily create job
                if job_id is None:
                    job_id = generate_job_id()
                    job_folder = os.path.join(JOBS_DIR, job_id)
                    os.makedirs(job_folder, exist_ok=True)
                dest_path = os.path.join(job_folder, original_name)
                f.save(dest_path)
                saved.append((dest_path, original_name))

        if not saved:
            return render_template('upload.html', job=None, error='Nisu pronađene podržane datoteke.')

        # Create job now
        with jobs_lock:
            job = {
                'id': job_id,
                'files': [],
                'options': {
                    'paper_size': 'A4',
                    'color': 'BW',
                    'duplex': 'No',
                    'copies': 1,
                },
                'price': 0.0,
                'stage': 'upload',
                'created_at': now_utc().isoformat(),
                'last_activity': time.time(),
                'assigned': False,
                'total_pages': 0,
            }
            add_files_to_job(job, saved)
            job['price'] = calc_price(job, job['options'])
            jobs[job_id] = job

        # Redirect to allow adding more files to this job if desired
        return redirect(url_for('upload_existing', job_id=job_id))

    # GET
    return render_template('upload.html', job=None)


@app.route('/upload/<job_id>', methods=['GET', 'POST'])
def upload_existing(job_id):
    """Continue uploading more files to an existing job."""
    job = ensure_job(job_id)

    if request.method == 'POST':
        files = request.files.getlist('files')
        if not files:
            return render_template('upload.html', job=job, error='Odaberite barem jednu datoteku.')

        job_folder = os.path.join(JOBS_DIR, job_id)
        os.makedirs(job_folder, exist_ok=True)

        saved = []
        for f in files:
            if f and allowed_file(f.filename):
                original_name = secure_filename(f.filename)
                dest_path = os.path.join(job_folder, original_name)
                f.save(dest_path)
                saved.append((dest_path, original_name))

        with jobs_lock:
            add_files_to_job(job, saved)
            job['price'] = calc_price(job, job['options'])

        return redirect(url_for('upload_existing', job_id=job_id))

    return render_template('upload.html', job=job)


@app.route('/print/<job_id>', methods=['GET', 'POST'])
def print_options(job_id):
    job = ensure_job(job_id)

    if request.method == 'POST':
        print(f"[POST /print/{job_id}] received")
        # Read options, compute price, spawn printer thread, redirect to done
        paper_size = request.form.get('paper_size', 'A4')
        color = request.form.get('color', 'BW')
        duplex = request.form.get('duplex', 'No')
        copies = int(request.form.get('copies', '1') or '1')

        with jobs_lock:
            job['options'] = {
                'paper_size': paper_size,
                'color': color,
                'duplex': duplex,
                'copies': copies,
            }
            # Parse custom page ranges for each file (optional)
            for idx, f in enumerate(job.get('files', [])):
                field = f'ranges_{idx}'
                text = (request.form.get(field, '') or '').strip()
                f['selected_text'] = text
                f['selected_pages'] = None
                if text:
                    total = int(f.get('pages', 0)) or 0
                    if total > 0:
                        sel = parse_page_ranges(text, total)
                        if sel:
                            f['selected_pages'] = sel
            job['price'] = calc_price(job, job['options'])

        # Start printing thread
        t = threading.Thread(target=print_job, args=(job_id,), daemon=True)
        t.start()
        print(f"[POST /print/{job_id}] started print thread")

        return redirect(url_for('done', job_id=job_id))

    # GET: auto-start printing if requested, else render options
    auto = request.args.get('auto')
    if auto:
        # If already printing/done, just go to done
        with jobs_lock:
            st = job.get('stage')
        if st in ('printing', 'done'):
            return redirect(url_for('done', job_id=job_id))

        # Use current options; compute price and fire the print thread
        with jobs_lock:
            job['price'] = calc_price(job, job['options'])
        t = threading.Thread(target=print_job, args=(job_id,), daemon=True)
        t.start()
        return redirect(url_for('done', job_id=job_id))

    # GET: ensure job is visible and compute current price
    with jobs_lock:
        job['price'] = calc_price(job, job['options'])

    return render_template('print_options.html', job=job)


@app.route('/done/<job_id>')
def done(job_id):
    job = ensure_job(job_id)
    return render_template('done.html', job=job)


@app.route('/job/<job_id>/status')
def job_status(job_id):
    job = ensure_job(job_id)
    return jsonify({'id': job_id, 'stage': job['stage']})


@app.route('/price_preview/<job_id>')
def price_preview(job_id):
    """AJAX helper to preview price when options change."""
    job = ensure_job(job_id)
    paper_size = request.args.get('paper_size', job['options'].get('paper_size', 'A4'))
    color = request.args.get('color', job['options'].get('color', 'BW'))
    duplex = request.args.get('duplex', job['options'].get('duplex', 'No'))
    copies = int(request.args.get('copies', job['options'].get('copies', 1)))
    options = {
        'paper_size': paper_size,
        'color': color,
        'duplex': duplex,
        'copies': copies,
    }
    price = calc_price(job, options)
    return jsonify({'price': price})


@app.route('/job/<job_id>/expire', methods=['POST'])
def expire_job(job_id):
    """Mark a job as deleted due to inactivity and remove its files.

    If the job is already printing/done, do not interrupt (returns 409).
    """
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            # Already gone; return ok so client can refresh
            return jsonify({'ok': True, 'status': 'missing'})
        if job.get('stage') in ('assigned', 'printing', 'done'):
            return jsonify({'ok': False, 'status': job.get('stage')}), 409
        job['stage'] = 'deleted'
    delete_job_files(job_id)
    return jsonify({'ok': True, 'status': 'deleted'})


if __name__ == '__main__':
    # Prefer waitress for serving in production
    try:
        from waitress import serve
        print(f"Serving on http://{get_local_ip()}:{PORT}")
        serve(app, host=HOST, port=PORT)
    except Exception:
        app.run(host=HOST, port=PORT, debug=True)
