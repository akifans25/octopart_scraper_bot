
"""
api_scraper.py — Octopart GraphQL API Scraper (MySQL Edition)

STRATEGY to bypass 1000-result API limit:
  1. For each manufacturer, first check grand total.
  2. If total <= 1000: paginate directly (simple).
  3. If total > 1000: use UseSearchQuery with child_category_agg to get
     all category IDs and their part counts.
  4. For each category with count <= 1000: paginate directly.
  5. For each category with count > 1000: drill down into its sub-categories
     recursively using child_category_agg again.

RESUME: Re-run anytime — slugs already marked 'done' in Manufacturer_url are skipped.
"""
import time
import json
import random
import requests
import mysql.connector
import browser_cookie3
from bs4 import BeautifulSoup as BS
from collections import deque
from loguru import logger
from datetime import datetime

# ─── MYSQL CONFIG ─────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     "13.201.205.150",
    "user":     "gd_data",
    "password": "GD@2025@softage",
    "database": "octopart",
}

# ─── API CONFIG ───────────────────────────────────────────────────────────────
API_URL     = "https://octopart.com/api/v4/internal"
DELAY       = (1.0, 2.0)
MAX_RETRIES = 3
LIMIT       = 20
MAX_START   = 980   # API hard limit: start must be 0–1000

# ─── COOKIES (handled dynamically via browser_cookie3) ───────────────────────
# (Removed hardcoded cookies string to prevent expiration issues)

HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'priority': 'u=0, i',
    'referer': 'https://octopart.com/manufacturers/amphenol',
    'sec-ch-ua': '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
    # 'cookie': '_pxvid=7a66353c-1877-11f1-bca4-06121b70748e; session=.eJxcy1EKwjAMgOG75NlKXbOu3WVK2qQY3DrB7kHEu4siCD798MH_gER7P0vrWqjr1lLfLtJgBi5i0ZV3ImKOzg4jTlyYrKunKnCAvK1HWrrua1KGGb7EersudE-NVvlp1UX-6DNhxup99IZDEINsqwl5EhNKHLHSNBTn4fkKAAD___02M9Y.aalPxw.jQZAyDqg7V3e4aaI2SHqviTBRoM; OptanonAlertBoxClosed=2026-03-05T09:41:38.977Z; __insp_wid=940536992; __insp_slim=1772703699268; __insp_nv=true; __insp_targlpu=aHR0cHM6Ly9vY3RvcGFydC5jb20vbWFudWZhY3R1cmVycy9vbXJvbg%3D%3D; __insp_targlpt=T21yb24gU3dpdGNoZXMgfCBTaG9wIE9tcm9uIEVsZWN0cm9uaWMgQ29tcG9uZW50cyAmIFBhcnRzIC0gT2N0b3BhcnQgRWxlY3Ryb25pYyBQYXJ0cw%3D%3D; ajs_anonymous_id=8ddb9eb1-8fe3-40cb-b5ea-4e5bf71fa009; __insp_norec_sess=true; _gcl_au=1.1.993088278.1772703702; _ga=GA1.1.267683408.1772703699; _uetvid=84c58c90187711f1878db1086bdcdb57; _biz_uid=bdecafc8f5b840c8e80becfe4536e756; _biz_nA=2; _biz_flagsA=%7B%22Version%22%3A1%2C%22ViewThrough%22%3A%221%22%2C%22XDomain%22%3A%221%22%7D; _biz_pendingA=%5B%5D; analytics_id=a6470492-bd30-4b6b-9678-8540a73a9917; _ga_SNYD338KXX=GS2.1.s1772706303$o2$g0$t1772706407$j60$l0$h0; _mkto_trk=id:817-SFW-071&token:_mch-octopart.com-d2a62a8fec0f6412e94135c625e0f8c7; _pxhd=Ufek6kmrYoZJpI-N6GKIQYAJmki42wGX8CZCMk4IXPuUEEjLwcddWZOkSx5plTPJoZKLWR5lm1FAh0LP6/18CQ==:CkFKyG9gV7KrSJjtTl5GnivRbATuL6DO4rFrjezAQbtUSbsPgCUjB-7hkMG8fnPp1Ym2ltq61d36xwW8UDn1PufRochMfrX3BQd-yXjwt9k=; cf_clearance=Jz2zVT71x0DRz1G8kL5Qf2qD3TYQNFJMT.7wQV5HSa8-1773128153-1.2.1.1-lwIJN2AXf4vcJDmQSLyEJo3afR_m0US3WjWR4uqSxpbnKslHUTANFzuI5GbWrns9yRFp476n6W_ip9Vab9ubt271vs_j6ASC9kmqvU7E.dNXeN4qzx_hVXwX8ycnrJZCaTWnxa8m76dvZso5Swgt4RXTT5vkQTYMp9NcZk7jQ7bgzc4FjK7WFYtp_e.e.6zWPs9L73mKu3aOgt8VqxPICz6fLyidDo9Z_CFrjt.63uQ; pxcts=c849be35-1c53-11f1-ad54-22ba1e0e652c; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Mar+10+2026+13%3A06%3A23+GMT%2B0530+(India+Standard+Time)&version=202403.1.0&browserGpcFlag=0&isIABGlobal=false&consentId=192d4b52-a030-4a5f-b3a7-e7dbd8edef72&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0002%3A1%2CC0004%3A1&hosts=H114%3A1%2CH29%3A1%2CH6%3A1%2CH11%3A1%2CH13%3A1%2CH14%3A1%2CH115%3A1%2CH16%3A1%2CH18%3A1%2CH25%3A1%2CH30%3A1&genVendors=&geolocation=AE%3BDU&AwaitingReconsent=false; g_state={"i_l":0,"i_ll":1773128184523,"i_b":"6fu1L85fRK037W+Xq5OZAu97W5a8YlfBukn8ordFA5Q","i_e":{"enable_itp_optimization":0}}; _px=iT0pMQfdsDx48nHatrfTjW5C+cxZwThjwdVwf5JLG5ku0Yvsq538SusMa7SLejzIj7HEJUpRSUv48lLfahh5Zg==:1000:rJFgT7gEzaf/t0lPIqo7NCHuzH3oZSov3oIsmxUiIlb0FeAd+Nx1ee0xSjhzIR1cKtp4qmQWHGKyeLDu8qHkY91Irv6hX2qiIpjJcRKwEqhwsMJGvOhQJQQSpqw2sm3WyhyCRbT1ejRWZKyGjfGGQYv7+9osZ2a+PAMmEqDn84ZEA4+xp+CPBl/GbsK0k2E1NCb39ODs013CtlgbJicVAOu6uHQVNpnxWbWKLvzfPLE0KnZcOZiud5DkwkEB64vgzL9M8yUXJy7vilfPvlEimQ==',
}
# ─── GRAPHQL QUERIES ──────────────────────────────────────────────────────────

# 1) Simple manufacturer lookup
MANUFACTURER_LOOKUP_QUERY = """
query ManufacturerSearch($q: String!) {
  search(q: $q country: "US" currency: "USD" limit: 1 start: 0) {
    results {
      part {
        manufacturer { id name }
      }
    }
  }
}
"""

# 2) AllCategories — fetches the full Octopart category tree
ALL_CATEGORIES_QUERY = """
query AllCategories {
  categories {
    id
    name
    parent_id
    path
  }
}
"""
# 3) Simple search — just parts (used for pagination)
SIMPLE_SEARCH_QUERY = """
query SuggestedFilterSearch(
  $filters: Map
  $country: String!
  $currency: String!
  $limit: Int
  $start: Int
) {
  search(
    filters: $filters
    country: $country
    currency: $currency
    limit: $limit
    start: $start
  ) {
    results {
      part {
        id
        mpn
        manufacturer { id name }
      }
    }
    total
  }
}
"""

# 4) Search with q (MPN keyword) — for prefix-based splitting of large categories
SIMPLE_SEARCH_QUERY_WITH_Q = """
query SuggestedFilterSearch(
  $filters: Map
  $q: String
  $country: String!
  $currency: String!
  $limit: Int
  $start: Int
) {
  search(
    q: $q
    filters: $filters
    country: $country
    currency: $currency
    limit: $limit
    start: $start
  ) {
    results {
      part {
        id
        mpn
        manufacturer { id name }
      }
    }
    total
  }
}
"""

# Characters used for q-prefix splitting (cover all alphanumeric MPNs)
Q_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'

# Global category cache — loaded once per run
_ALL_CATEGORIES = None   # list of {id, name, parent_id, path}



# ══════════════════════════════════════════════════════════════════════════════
# MYSQL HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def get_conn():
    return mysql.connector.connect(**DB_CONFIG)


def setup_db():
    conn = get_conn()
    cur  = conn.cursor()

    # Add scrape_status column if not present
    try:
        cur.execute("""
            ALTER TABLE Manufacturer_url
            ADD COLUMN scrape_status VARCHAR(20) DEFAULT 'pending'
        """)
        conn.commit()
        logger.info("Added scrape_status column to Manufacturer_url")
    except mysql.connector.errors.DatabaseError:
        pass

    # Create output table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Scraped_Parts (
            id                INT AUTO_INCREMENT PRIMARY KEY,
            part_id           VARCHAR(100) UNIQUE,
            mpn               VARCHAR(200),
            manufacturer_id   VARCHAR(50),
            manufacturer_name VARCHAR(200),
            source_slug       VARCHAR(200),
            scraped_at        DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    logger.info("DB ready — Scraped_Parts table OK")


def get_next_url():
    """Atomically check out one pending URL for this worker."""
    conn = get_conn()
    cur  = conn.cursor()
    
    # Find the next pending row
    cur.execute("""
        SELECT id, url 
        FROM Manufacturer_url 
        WHERE scrape_status = 'pending' OR scrape_status IS NULL 
        ORDER BY id 
        LIMIT 1
    """)
    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        return None
        
    row_id, url = row[0], row[1]
    slug = url.rstrip("/").split("/")[-1]
    
    # Mark it as in_progress to claim it atomically
    cur.execute(
        "UPDATE Manufacturer_url SET scrape_status='in_progress' WHERE id=%s", 
        (row_id,)
    )
    conn.commit()
    cur.close()
    conn.close()
    return (row_id, url, slug)


def mark_url_done(row_id, status="done"):
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute(
        "UPDATE Manufacturer_url SET scrape_status=%s WHERE id=%s",
        (status, row_id)
    )
    conn.commit()
    cur.close()
    conn.close()


def bulk_insert_parts(rows):
    """rows = list of (part_id, mpn, mfr_id, mfr_name, slug)"""
    if not rows:
        return
    conn = get_conn()
    cur  = conn.cursor()
    cur.executemany("""
        INSERT IGNORE INTO Scraped_Parts
            (part_id, mpn, manufacturer_id, manufacturer_name, source_slug)
        VALUES (%s, %s, %s, %s, %s)
    """, rows)
    conn.commit()
    cur.close()
    conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# HTTP / GRAPHQL HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def get_auto_session():
    """
    Creates a requests session and loads cookies directly from the local browser
    using browser_cookie3. This grabs the necessary Cloudflare/PerimeterX cookies.
    """
    s = requests.Session()
    s.headers.update(HEADERS)
    
    try:
        logger.info("Extracting cookies from local browser for Octopart...")
        # Try Firefox, Chrome, and Edge
        cj = None
        loaders = [
            ("Firefox", browser_cookie3.firefox),
            ("Chrome", browser_cookie3.chrome),
            ("Edge", browser_cookie3.edge)
        ]
        
        for name, loader in loaders:
            try:
                temp_cj = loader(domain_name='octopart.com')
                if len(temp_cj) > 0:
                    cj = temp_cj
                    logger.info(f"Cookies loaded from {name} ({len(cj)} cookies found).")
                    break
                else:
                    logger.debug(f"No cookies found in {name}.")
            except Exception as e:
                logger.debug(f"Could not load cookies from {name}: {e}")
                
        if not cj:
            logger.error("Could not find Octopart cookies in Firefox, Chrome, or Edge.")

        
        if cj:
            s.cookies.update(cj)
            logger.info(f"Successfully loaded {len(s.cookies)} cookies!")
            logger.info("--- EXTRACTED IMPORTANT COOKIES ---")
            for c in s.cookies:
                if c.name in ['cf_clearance', '_px', '_pxvid', 'session', 'OptanonConsent']:
                    val = c.value if c.value else ""
                    end_val = val[-10:] if len(val) >= 10 else val
                    logger.info(f"{c.name}: {val[:30]}... (ends in {end_val})")
            logger.info("-----------------------------------")
            
            # Simple test to verify cookies work
            logger.info("Verifying cookies with Octopart...")
            r = s.get("https://octopart.com/", timeout=15)
            if r.status_code == 403:
                logger.warning("Octopart returned 403 Forbidden even with browser cookies.")
            else:
                logger.info(f"Verification successful! (Status: {r.status_code})")
        else:
            logger.warning("No browser cookies found! Cloudflare will likely block requests.")
            
    except Exception as e:
        logger.error(f"Failed to fetch browser cookies: {e}")
        
    return s

session = get_auto_session()


def api_post(payload, label=""):
    """POST GraphQL with retry + rate-limit handling."""
    global session
    for attempt in range(1, MAX_RETRIES + 1):
        time.sleep(random.uniform(*DELAY))
        try:
            r = session.post(
                API_URL,
                json=payload,
                params={"operation": payload.get("operationName", "")},
                timeout=30
            )
            if r.status_code == 403:
                logger.warning(f"[{label}] 403 — Blocked/Expired. Refreshing cookies from browser (attempt {attempt})")
                session = get_auto_session()
                time.sleep(15)
                continue
            data = r.json()
            if "errors" in data:
                logger.warning(f"[{label}] API error: {data['errors'][0].get('message','?')}")
                time.sleep(15 * attempt)
                continue
            return data
        except Exception as e:
            logger.warning(f"[{label}] Attempt {attempt}/{MAX_RETRIES}: {e}")
            time.sleep(10 * attempt)
    return None


def resolve_manufacturer_id(slug):
    search_term = slug.replace("-", " ")
    payload = {
        "query": MANUFACTURER_LOOKUP_QUERY,
        "variables": {"q": search_term},
        "operationName": "ManufacturerSearch"
    }
    data = api_post(payload, label=slug)
    if not data:
        return None, None
    try:
        results = data["data"]["search"]["results"]
        if not results:
            return None, None
        slug_clean = slug.replace("-", "").lower()
        for item in results:
            mfr = item.get("part", {}).get("manufacturer", {})
            if slug_clean in mfr.get("name", "").replace(" ", "").lower():
                return mfr["id"], mfr["name"]
        mfr = results[0]["part"]["manufacturer"]
        return mfr["id"], mfr["name"]
    except (KeyError, IndexError, TypeError):
        return None, None


# ══════════════════════════════════════════════════════════════════════════════
# CORE FETCH FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════
def fetch_total_for_filter(filters, label):
    """Quick check: how many parts match this filter. Returns total int."""
    payload = {
        "query": SIMPLE_SEARCH_QUERY,
        "variables": {"filters": filters, "country": "IN", "currency": "USD", "limit": 1, "start": 0},
        "operationName": "SuggestedFilterSearch"
    }
    data = api_post(payload, label=label)
    if not data:
        return 0
    try:
        return data["data"]["search"].get("total", 0)
    except (KeyError, TypeError):
        return 0

def paginate_filter(filters, label, mfr_id, mfr_name, slug):
    """
    Paginate a filter combo up to MAX_START (1000 items max per API limit).
    Returns list of (part_id, mpn, mfr_id, mfr_name, slug).
    """
    rows  = []
    start = 0
    page  = 0

    while True:
        payload = {
            "query": SIMPLE_SEARCH_QUERY,
            "variables": {
                "filters":  filters,
                "country":  "IN",
                "currency": "USD",
                "limit":    LIMIT,
                "start":    start
            },
            "operationName": "SuggestedFilterSearch"
        }
        data = api_post(payload, label=f"{label}@{start}")
        if not data:
            break

        try:
            s       = data["data"]["search"]
            total   = s.get("total", 0)
            results = s.get("results", [])
        except (KeyError, TypeError):
            break

        if not results:
            break

        page += 1
        for item in results:
            part     = item.get("part", {})
            part_id  = part.get("id")
            mpn      = part.get("mpn", "")
            p_mfr    = part.get("manufacturer", {})
            p_mfr_id = p_mfr.get("id", mfr_id)
            p_mfr_nm = p_mfr.get("name", mfr_name)
            if part_id:
                rows.append((part_id, mpn, p_mfr_id, p_mfr_nm, slug))

        logger.info(
            f"  [{label}] page={page} start={start} "
            f"got={len(results)} total={total} collected={len(rows)}"
        )

        start += LIMIT
        if start > MAX_START or start >= total:
            break

    return rows


def paginate_filter_with_q(filters, q, label, mfr_id, mfr_name, slug, seen_ids):
    """Like paginate_filter but adds q keyword filter. Deduplicates via seen_ids."""
    rows  = []
    start = 0
    page  = 0

    while True:
        payload = {
            "query": SIMPLE_SEARCH_QUERY_WITH_Q,
            "variables": {
                "filters":  filters,
                "q":        q,
                "country":  "IN",
                "currency": "USD",
                "limit":    LIMIT,
                "start":    start
            },
            "operationName": "SuggestedFilterSearch"
        }
        data = api_post(payload, label=f"{label}@{start}")
        if not data:
            break
        try:
            sr      = data["data"]["search"]
            total   = sr.get("total", 0)
            results = sr.get("results", [])
        except (KeyError, TypeError):
            break
        if not results:
            break

        page += 1
        for item in results:
            part     = item.get("part", {})
            part_id  = part.get("id")
            if not part_id or part_id in seen_ids:
                continue
            seen_ids.add(part_id)
            mpn      = part.get("mpn", "")
            p_mfr    = part.get("manufacturer", {})
            rows.append((part_id, mpn, p_mfr.get("id", mfr_id), p_mfr.get("name", mfr_name), slug))

        logger.info(f"  [q={q!r}|{label}] page={page} start={start} got={len(results)} total={total} new={len(rows)}")
        start += LIMIT
        if start > MAX_START or start >= total:
            break

    return rows


def q_prefix_split(filters, label, mfr_id, mfr_name, slug, seen_ids, prefix='', depth=0):
    """
    Recursively split a large result set using q-prefix search.
    - Try each char in Q_ALPHABET as a query prefix
    - If a prefix group has <=1000 parts: paginate and collect
    - If a prefix group has >1000: recurse deeper (up to MAX_Q_DEPTH)
    - Every alphanumeric MPN will be covered by at least one prefix
    """
    MAX_Q_DEPTH = 3  # e.g. prefix can be up to 3 chars: 'A', 'AB', 'ABC'
    all_rows = []

    for char in Q_ALPHABET:
        q = prefix + char

        # Quick count probe
        payload = {
            "query": SIMPLE_SEARCH_QUERY_WITH_Q,
            "variables": {"filters": filters, "q": q, "country": "IN", "currency": "USD", "limit": 1, "start": 0},
            "operationName": "SuggestedFilterSearch"
        }
        data = api_post(payload, label=f"{label}|q={q}_probe")
        count = 0
        if data:
            try:
                count = data["data"]["search"].get("total", 0)
            except (KeyError, TypeError):
                pass

        if count == 0:
            continue

        if count <= MAX_START + LIMIT or depth >= MAX_Q_DEPTH:
            rows = paginate_filter_with_q(filters, q, f"{label}|q={q}", mfr_id, mfr_name, slug, seen_ids)
            bulk_insert_parts(rows)
            all_rows.extend(rows)
            logger.info(f"[{label}] q={q!r}: {count} -> saved {len(rows)} (total so far {len(all_rows)})")
        else:
            logger.info(f"[{label}] q={q!r}: {count} parts > 1000, drilling deeper...")
            sub_rows = q_prefix_split(filters, label, mfr_id, mfr_name, slug, seen_ids, prefix=q, depth=depth+1)
            all_rows.extend(sub_rows)

    return all_rows

# ══════════════════════════════════════════════════════════════════════════════
def load_all_categories():
    """
    Load full Octopart category tree using AllCategories query.
    Returns list of {id, name, parent_id, path}.
    Cached globally after first call.
    """
    global _ALL_CATEGORIES
    if _ALL_CATEGORIES is not None:
        return _ALL_CATEGORIES

    payload = {"query": ALL_CATEGORIES_QUERY, "operationName": "AllCategories"}
    data = api_post(payload, label="AllCategories")
    if not data:
        logger.warning("Failed to load AllCategories — will be empty")
        _ALL_CATEGORIES = []
        return []

    cats = (data.get("data") or {}).get("categories") or []
    _ALL_CATEGORIES = cats
    logger.info(f"AllCategories loaded: {len(cats)} categories")
    return cats


def get_leaf_category_ids():
    """
    Return leaf category IDs from the Octopart category tree.
    Leaf = categories that have no children (deepest level).
    These give the finest granularity for manufacturer splitting.
    """
    cats = load_all_categories()
    if not cats:
        return []
    parent_ids = {c.get("parent_id") for c in cats if c.get("parent_id")}
    # Leaf = not a parent of any other category
    leaves = [c for c in cats if c.get("id") not in parent_ids and c.get("id") != "4161"]
    return [c["id"] for c in leaves]


def category_split_scrape(mfr_id, mfr_name, slug):
    """
    For manufacturers with >1000 parts:
    1. Get all leaf category IDs from AllCategories
    2. For each, probe with manufacturer_id + category_id filter
    3. If total > 0 and <= 1000: paginate directlyx`    
    4. If total > 1000: recursively use parent categories (rare edge case)
    Returns total parts collected.
    """
    leaf_ids = get_leaf_category_ids()
    if not leaf_ids:
        logger.warning(f"[{slug}] No categories available — fetching max 1000")
        rows = paginate_filter({"manufacturer_id": [str(mfr_id)]}, slug, mfr_id, mfr_name, slug)
        bulk_insert_parts(rows)
        return len(rows)

    logger.info(f"[{slug}] Category split: probing {len(leaf_ids)} leaf categories...")
    total_saved = 0
    cats_with_parts = 0

    for i, cat_id in enumerate(leaf_ids):
        filters = {"manufacturer_id": [str(mfr_id)], "category_id": [cat_id]}

        # Quick probe — limit=1 to check if this category has any parts
        count = fetch_total_for_filter(filters, f"{slug}|cat={cat_id}_probe")

        if count == 0:
            continue  # Skip empty categories

        cats_with_parts += 1
        logger.info(f"[{slug}] cat={cat_id} has {count} parts ({i+1}/{len(leaf_ids)})")

        if count <= MAX_START + LIMIT:
            rows = paginate_filter(filters, f"{slug}|cat={cat_id}", mfr_id, mfr_name, slug)
            bulk_insert_parts(rows)
            total_saved += len(rows)
        else:
            # Large leaf category — use q-prefix splitting to get ALL parts
            logger.info(f"[{slug}] cat={cat_id} has {count} > 1000 — q-prefix splitting...")
            seen_ids = set()
            q_rows = q_prefix_split(filters, f"{slug}|cat={cat_id}", mfr_id, mfr_name, slug, seen_ids)
            total_saved += len(q_rows)
            logger.info(f"[{slug}] cat={cat_id}: q-split complete, {len(q_rows)} parts")


    logger.success(f"[{slug}] Category split done: {cats_with_parts} categories, {total_saved} parts total")
    return total_saved





# ══════════════════════════════════════════════════════════════════════════════
# MAIN SCRAPE LOGIC — manufacturer fetch with category splitting
# ══════════════════════════════════════════════════════════════════════════════
def scrape_brand(row_id, slug):
    logger.info(f"\n[{slug}] Resolving manufacturer_id...")
    mfr_id, mfr_name = resolve_manufacturer_id(slug)

    if not mfr_id:
        logger.error(f"[{slug}] Could not resolve manufacturer_id — skipping")
        mark_url_done(row_id, status="failed")
        return

    logger.info(f"[{slug}] Resolved → id={mfr_id} | name={mfr_name}")

    # Quick total count
    base_filters = {"manufacturer_id": [str(mfr_id)]}
    grand_total  = fetch_total_for_filter(base_filters, slug)
    logger.info(f"[{slug}] Grand total parts: {grand_total}")

    if grand_total == 0:
        mark_url_done(row_id, status="done")
        logger.info(f"[{slug}] 0 parts — skipping")
        return

    if grand_total <= MAX_START + LIMIT:
        # Small enough — paginate directly
        rows = paginate_filter(base_filters, slug, mfr_id, mfr_name, slug)
        bulk_insert_parts(rows)
        total_saved = len(rows)
    else:
        # Large manufacturer — split by leaf categories from AllCategories
        logger.info(f"[{slug}] {grand_total} parts > 1000 — using AllCategories split...")
        total_saved = category_split_scrape(mfr_id, mfr_name, slug)

    mark_url_done(row_id, status="done")
    logger.success(f"[{slug}] ✓ DONE — {total_saved} parts saved (API total={grand_total})")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("Octopart API Scraper — MySQL Edition (category drilldown)")
    logger.info("Reads from : octopart.Manufacturer_url")
    logger.info("Saves to   : octopart.Scraped_Parts")
    logger.info("=" * 60)

    setup_db()
    
    # Count total for progress tracking
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM Manufacturer_url")
    total = cur.fetchone()[0]
    cur.close()
    conn.close()
    
    i = 0
    while True:
        brand = get_next_url()
        if not brand:
            logger.info("No more pending brands — all done!")
            break
            
        row_id, url, slug = brand
        i += 1
        logger.info(f"\n{'='*55}")
        logger.info(f"[{i} / ~{total} total] Picked up: {slug}")
        logger.info(f"{'='*55}")
        
        try:
            # scrape_brand automatically marks 'done' on success
            scrape_brand(row_id, slug)
        except Exception as e:
            logger.error(f"[{slug}] Unexpected error: {e}")
            mark_url_done(row_id, status="failed")

    # Final summary
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM Scraped_Parts")
    total_parts = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM Manufacturer_url WHERE scrape_status='done'")
    done_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM Manufacturer_url WHERE scrape_status='failed'")
    fail_count = cur.fetchone()[0]
    cur.close()
    conn.close()

    logger.info("\n" + "=" * 60)
    logger.info("FINAL SUMMARY")
    logger.info(f"  Brands done   : {done_count}")
    logger.info(f"  Brands failed : {fail_count}")
    logger.info(f"  Total parts   : {total_parts:,}")
    logger.info("=" * 60)
