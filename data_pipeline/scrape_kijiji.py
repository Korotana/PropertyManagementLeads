from pathlib import Path
from datetime import datetime
import re
import pandas as pd
from parsel import Selector

RAW_DIR = Path("data/raw")
PROC_DIR = Path("data/processed")
PROC_DIR.mkdir(parents=True, exist_ok=True)

# NOTE: Kijiji changes classes periodically. We target semantic attributes when possible.
LISTING_CARD_SEL = "div.search-item"  # fallback; adjust if empty
TITLE_SEL = "a.title::text, a[title]::attr(title)"  # try text then title attr
URL_SEL = "a.title::attr(href), a[data-listing-id]::attr(href)"
PRICE_SEL = ".price::text"
LOC_SEL = ".location, .third-line span::text"
POSTED_SEL = ".date-posted::text, time::attr(datetime)"

def clean(t):
    if t is None:
        return None
    return re.sub(r"\s+", " ", t).strip()

def parse_search_html(html_text: str):
    sel = Selector(html_text)
    rows = []
    for card in sel.css(LISTING_CARD_SEL):
        title = clean("".join(card.css(TITLE_SEL).getall()) or None)
        url_rel = clean((card.css(URL_SEL).get() or "")[:500])
        price_text = clean("".join(card.css(PRICE_SEL).getall()) or None)
        loc_text = clean("".join(card.css(LOC_SEL).getall()) or None)
        posted_text = clean("".join(card.css(POSTED_SEL).getall()) or None)

        if not title and not url_rel:
            continue

        url = None
        if url_rel:
            url = url_rel if url_rel.startswith("http") else f"https://www.kijiji.ca{url_rel}"

        rows.append({
            "source": "kijiji",
            "url": url,
            "title": title,
            "price_text": price_text,
            "location_text": loc_text,
            "posted_text": posted_text,
        })
    return rows

def parse_all_snapshots() -> Path:
    files = sorted(RAW_DIR.glob("kijiji_search_*.html"))
    if not files:
        raise SystemExit("No raw HTML snapshots found. Run the snapshot step first.")
    all_rows = []
    for f in files:
        html = f.read_text(encoding="utf-8", errors="ignore")
        rows = parse_search_html(html)
        all_rows.extend(rows)
    df = pd.DataFrame(all_rows).drop_duplicates(subset=["url"]).reset_index(drop=True)
    df["scraped_ts"] = datetime.now().isoformat(timespec="seconds")
    out = PROC_DIR / "listings_raw.parquet"
    df.to_parquet(out, index=False)
    return out

if __name__ == "__main__":
    out = parse_all_snapshots()
    print("Wrote:", out)
