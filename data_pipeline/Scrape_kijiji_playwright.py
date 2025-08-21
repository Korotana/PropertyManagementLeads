from pathlib import Path
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential
from playwright.sync_api import sync_playwright

SEARCH_URL_TPL = "https://www.kijiji.ca/b-apartments-condos/saskatoon/page-{page}/c37l1700197?ad=offering&sort=dateDesc"
RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

CARD_HINTS = [
    "div.search-item",
    "div[data-listing-id]",
    "li[data-testid='listing-card']",
    "div[data-qa-id='result-item']",
]

@retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=25))
def fetch_and_wait(page, url: str) -> str:
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    # Try a few likely selectors; if none appear, scroll + wait a bit
    for _ in range(3):
        for sel in CARD_HINTS:
            try:
                page.wait_for_selector(sel, timeout=4000)
                break
            except Exception:
                pass
        # nudge the page to load lazy content
        page.mouse.wheel(0, 4000)
        page.wait_for_timeout(800)
    return page.content()

def snapshot_search_pages(max_pages: int = 2) -> list[Path]:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_files = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=[
            "--disable-blink-features=AutomationControlled",
        ])
        context = browser.new_context(
            locale="en-CA",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 860},
        )
        page = context.new_page()
        for i in range(1, max_pages + 1):
            url = SEARCH_URL_TPL.format(page=i)
            html = fetch_and_wait(page, url)

            # Save HTML and a screenshot for debugging
            html_path = RAW_DIR / f"kijiji_search_p{i}_{ts}.html"
            img_path  = RAW_DIR / f"kijiji_search_p{i}_{ts}.png"
            html_path.write_text(html, encoding="utf-8")
            page.screenshot(path=str(img_path), full_page=True)

            out_files.append(html_path)
        browser.close()
    return out_files

if __name__ == "__main__":
    files = snapshot_search_pages(max_pages=2)
    print("Saved:", *map(str, files), sep="\n  ")
