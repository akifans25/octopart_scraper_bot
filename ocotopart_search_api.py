"""
Component Search Engine - Bulk CAD Model Downloader (MySQL Integrated)
------------------------------------------------------
REQUIREMENT (one-time): Log into componentsearchengine.com in your Firefox browser.
The script will read those cookies automatically.

DATABASE:
- Fetches parts from `octopart.part_info` where `cad_status` is 'pending'.
- Atomically marks parts as 'in_process' to support distributed runs.
- Updates status to 'done' or 'failed' (with error) after processing.
"""

import os, re, time, json, zipfile, shutil
import browser_cookie3
import mysql.connector
from playwright.sync_api import sync_playwright

# ─── MYSQL CONFIG ─────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     "13.201.205.150",
    "user":     "gd_data",
    "password": "GD@2025@softage",
    "database": "octopart",
}

# ─── CONFIG ────────────────────────────────────────────────────────────────────
DOWNLOAD_DIR   = os.path.join(os.path.dirname(__file__), "download")
STP_FILES_DIR  = os.path.join(DOWNLOAD_DIR, "STP_FILES")
TEMP_EXTRACT_DIR = os.path.join(DOWNLOAD_DIR, "tmp_extract")
DELAY_BETWEEN  = 180  # seconds between parts
RATE_WAIT      = 300  # seconds to wait when rate limited
MAX_RETRIES    = 3    # retries per part

# ─────────────────────────────────────────────────────────────────────────────

def extract_step_file(zip_path, part_number):
    """
    Extracts the ZIP, finds a file ending in .stp/.step inside a 'CD' folder,
    and moves it to STP_FILES_DIR.
    """
    try:
        os.makedirs(STP_FILES_DIR, exist_ok=True)
        if os.path.exists(TEMP_EXTRACT_DIR):
            shutil.rmtree(TEMP_EXTRACT_DIR)
        os.makedirs(TEMP_EXTRACT_DIR)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(TEMP_EXTRACT_DIR)

        # Search for .stp/.step files EXCLUSIVELY in '3D' folders
        found_step = None
        for root, dirs, files in os.walk(TEMP_EXTRACT_DIR):
            path_parts = root.lower().replace(TEMP_EXTRACT_DIR.lower(), "").strip(os.sep).split(os.sep)
            if '3d' in path_parts:
                for file in files:
                    if file.lower().endswith(('.stp', '.step')):
                        found_step = os.path.join(root, file)
                        break
            if found_step: break

        if found_step:
            # Clean filename: remove illegal chars
            clean_name = re.sub(r'[\\/:*?"<>|]', '_', part_number)
            dest_path = os.path.join(STP_FILES_DIR, f"{clean_name}.step")
            shutil.copy2(found_step, dest_path)
            return dest_path
        
        return None
    except Exception as e:
        print(f"  ⚠  Extraction error: {e}")
        return None
    finally:
        if os.path.exists(TEMP_EXTRACT_DIR):
            shutil.rmtree(TEMP_EXTRACT_DIR, ignore_errors=True)


def get_db_conn():
    return mysql.connector.connect(**DB_CONFIG)

def setup_db():
    """Ensure cad_status column exists in part_info."""
    conn = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        try:
            cur.execute("ALTER TABLE part_info ADD COLUMN cad_status VARCHAR(50) DEFAULT 'pending'")
            conn.commit()
            print("✓ Added cad_status column to part_info.")
        except mysql.connector.Error:
            pass # Already exists
        
        # Surgical update: only update what's necessary to avoid long locks
        cur.execute("UPDATE part_info SET cad_status = 'pending' WHERE cad_status IS NULL LIMIT 100")
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"  ⚠  DB Setup warning: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()

def claim_next_part():
    """
    Atomically get one 'pending' part and mark it 'in_process'.
    Returns (id, part_number) or (None, None).
    """
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        # SKIP LOCKED allows multiple systems to pick different rows instantly without waiting
        cur.execute("SELECT id, part_number FROM part_info WHERE cad_status = 'pending' LIMIT 1 FOR UPDATE SKIP LOCKED")
        row = cur.fetchone()
        if not row:
            return None, None
        
        part_id, part_number = row
        cur.execute("UPDATE part_info SET cad_status = 'in_process' WHERE id = %s", (part_id,))
        conn.commit()
        return part_id, part_number
    finally:
        cur.close()
        conn.close()

def update_part_status(part_id, status):
    """Update part status in DB."""
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("UPDATE part_info SET cad_status = %s WHERE id = %s", (status, part_id))
    conn.commit()
    cur.close()
    conn.close()

def load_cookies():
    for fn, name in [(browser_cookie3.firefox, "Firefox"),
                     (browser_cookie3.chrome, "Chrome")]:
        try:
            jar = fn(domain_name="componentsearchengine.com")
            cookies = list(jar)
            if cookies:
                print(f"✓ Loaded {len(cookies)} cookies from {name}.")
                return cookies
        except Exception:
            continue
    return []

def inject_cookies(context, raw_cookies):
    context.add_cookies([
        {"name": c.name, "value": c.value,
         "domain": "componentsearchengine.com", "path": c.path or "/"}
        for c in raw_cookies
    ])

def check_cloudflare(page):
    """Wait if Cloudflare shows a challenge page."""
    try:
        if "Just a moment..." in page.title():
            print("  ⚠  Cloudflare! Waiting for auto-resolve...")
            for _ in range(30):
                time.sleep(1)
                if "Just a moment..." not in page.title():
                    break
    except Exception:
        # Happens if page navigates while checking title
        pass

def find_manufacturer(page, part_number):
    """Use full browser to search and find the part-view URL."""
    page.goto(f"https://componentsearchengine.com/search?term={part_number}",
              wait_until="domcontentloaded")
    check_cloudflare(page)

    try:
        page.wait_for_selector("a[href*='/part-view/']", timeout=15_000)
    except Exception:
        return None, None

    link_el = page.locator("a[href*='/part-view/']").first
    if link_el.count() == 0:
        return None, None
    href = link_el.get_attribute("href") or ""
    parts = href.strip("/").split("/")
    if len(parts) >= 3:
        return parts[1], parts[2]
    return None, None

def get_download_info(page, mpn, manufacturer):
    page.goto(f"https://componentsearchengine.com/part-view/{mpn}/{manufacturer}",
              wait_until="domcontentloaded")
    check_cloudflare(page)
    time.sleep(2)

    html = page.content()
    m = re.search(r'data-samac-id=["\'](\d+)["\']', html)
    model_id = m.group(1) if m else None

    dl_btn = page.locator("a.ecad-model-button")
    if dl_btn.count() == 0:
        return model_id, None, False

    href = dl_btn.get_attribute("href") or ""
    is_auth = "/register" not in href and "/signin" not in href
    return model_id, href, is_auth

def download_via_request(context, mpn, manufacturer, model_id):
    try:
        r = context.request.get(
            "https://componentsearchengine.com/partApi/model/download",
            params={"from": f"/part-view/{mpn}/{manufacturer}", "id": model_id},
            headers={"Referer": f"https://componentsearchengine.com/part-view/{mpn}/{manufacturer}"}
        )
        ct = r.headers.get("content-type", "")
        if "text/plain" in ct or "text/html" in ct:
            return None, r.text()[:200]
        if "zip" in ct or "octet" in ct or "application" in ct:
            return r.body(), None
        return None, f"unknown ct: {ct}"
    except Exception as e:
        return None, f"Network error: {str(e)}"

def process_part(page, context, mpn, manufacturer, model_id):
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    filename = f"{mpn}_{manufacturer}.zip".replace("/", "-")
    filepath = os.path.join(DOWNLOAD_DIR, filename)

    for attempt in range(1, MAX_RETRIES + 1):
        data, err = download_via_request(context, mpn, manufacturer, model_id)
        if data:
            with open(filepath, "wb") as f:
                f.write(data)
            return filepath, None
        
        if err and "rate" in err.lower():
            print(f"  ⚠ Rate limited (attempt {attempt}/{MAX_RETRIES}). Waiting {RATE_WAIT}s...")
            time.sleep(RATE_WAIT)
            # Re-load cookies in case of expiry
            fresh = load_cookies()
            if fresh: inject_cookies(context, fresh)
        else:
            return None, err or "unknown error"
    
    return None, "Rate limited"

def run():
    print("=" * 50)
    print(" DB-DRIVEN COMPONENT DOWNLOADER ")
    print("=" * 50)
    
    setup_db()
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    raw_cookies = load_cookies()
    if not raw_cookies:
        print("✗ No cookies found. Please log in to componentsearchengine.com in Firefox.")
        return

    with sync_playwright() as pw:
        browser = pw.firefox.launch(headless=False)
        context = browser.new_context(accept_downloads=True)
        inject_cookies(context, raw_cookies)
        page = context.new_page()

        # Check login
        page.goto("https://componentsearchengine.com/", wait_until="domcontentloaded")
        time.sleep(2)
        check_cloudflare(page)
        is_signed_in = page.locator(".user-actions a.button.secondary").count() == 0
        if not is_signed_in:
            print("⚠ Not logged in. Please log in in Firefox and rerun.")
            browser.close()
            return

        print("✓ Login verified. Starting loop...\n")

        try:
            while True:
                part_id, part_number = claim_next_part()
                if not part_number:
                    print("✓ No more 'pending' parts in database.")
                    break

                print(f"[*] Processing: {part_number} (id={part_id})")
                
                try:
                    # Step 1: Search
                    mpn, mfr = find_manufacturer(page, part_number)
                    if not mpn:
                        print("  ✗ Not found in search")
                        update_part_status(part_id, "failed: Not found")
                        time.sleep(DELAY_BETWEEN)
                        continue
                    
                    # Step 2: Info
                    model_id, _ , is_auth = get_download_info(page, mpn, mfr)
                    if not model_id or not is_auth:
                        status = "failed: No model" if not model_id else "failed: Not auth"
                        print(f"  ✗ {status}")
                        update_part_status(part_id, status)
                        time.sleep(DELAY_BETWEEN)
                        continue
                    
                    # Step 3: Download
                    filepath, err = process_part(page, context, mpn, mfr, model_id)
                    if filepath:
                        print(f"  ✓ Downloaded: {os.path.basename(filepath)}")
                        # --- EXTRACTION STEP ---
                        step_file = extract_step_file(filepath, part_number)
                        if step_file:
                            print(f"  ✓ STEP extracted: {os.path.basename(step_file)}")
                        else:
                            print(f"  ⚠ No STEP file extracted from '3D' folder.")
                        # -----------------------
                        update_part_status(part_id, "completed")
                    else:
                        print(f"  ✗ {err}")
                        update_part_status(part_id, f"failed: {err}")
                except Exception as e:
                    print(f"  ✗ Unexpected error for {part_number}: {e}")
                    update_part_status(part_id, f"failed: error")

                time.sleep(DELAY_BETWEEN)
        finally:
            browser.close()

if __name__ == "__main__":
    run()
