from pathlib import Path
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential
from playwright.sync_api import sync_playwright

SEARCH_URL_TPL = "https://www.kijiji.ca/b-apartments-condos/saskatoon/page-{page}/c37l1700197?ad=offering&sort=dateDesc"
RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

@retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=20))
def fetch_search_page(pw_page, url: str) -> str:
    pw_page.goto(url, wait_until="domcontentloaded", timeout=45000)
    pw_page.wait_for_timeout(1000)  # small human-like pause
    return pw_page.content()

def snapshot_search_pages(max_pages: int = 2) -> list[Path]:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_files = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context()
        page = context.new_page()
        for pi in range(1, max_pages + 1):
            url = SEARCH_URL_TPL.format(page=pi)
            html = fetch_search_page(page, url)
            fpath = RAW_DIR / f"kijiji_search_p{pi}_{ts}.html"
            fpath.write_text(html, encoding="utf-8")
            out_files.append(fpath)
        browser.close()
    return out_files

if __name__ == "__main__":
    files = snapshot_search_pages(max_pages=2)
    print(f"Saved {len(files)} files:")
    for f in files:
        print(" -", f)
