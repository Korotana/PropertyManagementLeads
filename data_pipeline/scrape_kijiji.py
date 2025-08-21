# data_pipeline/scrape_kijiji.py
from pathlib import Path
from datetime import datetime
import re
import pandas as pd
from parsel import Selector

RAW_DIR = Path("data/raw")
PROC_DIR = Path("data/processed")
PROC_DIR.mkdir(parents=True, exist_ok=True)

CARD_SELS = ["section[data-testid='listing-card']"]
TITLE_SELS = ["a[data-testid='listing-link']::text","h3::text"]
URL_SELS   = ["a[data-testid='listing-link']::attr(href)","a::attr(href)"]
PRICE_SELS = ["p[data-testid='listing-price']::text","span:contains('$')::text"]
LOC_SELS   = ["span[data-testid='location']::text","address::text"]
POSTED_SELS= ["span[data-testid='listing-date']::text","time::attr(datetime)"]

def clean(t):
    if t is None: return None
    return re.sub(r"\s+", " ", t).strip()

def first_nonempty_text(card, css_list):
    for sel in css_list:
        got = card.css(sel).getall()
        if got:
            text = clean("".join(got))
            if text:
                return text
    return None

def first_attr(card, css_list):
    for sel in css_list:
        val = card.css(sel).get()
        if val and (val := clean(val)):
            return val
    return None

def absolutize(url_rel):
    if not url_rel: return None
    return url_rel if url_rel.startswith("http") else f"https://www.kijiji.ca{url_rel}"

def parse_search_html(html_text: str):
    sel = Selector(html_text)
    cards = []
    for cs in CARD_SELS:
        cards = sel.css(cs)
        if len(cards) > 0:
            break

    rows = []
    for card in cards:
        title = first_nonempty_text(card, TITLE_SELS)
        url_rel = first_attr(card, URL_SELS)
        url = absolutize(url_rel)
        price_text = first_nonempty_text(card, PRICE_SELS)
        loc_text = first_nonempty_text(card, LOC_SELS)
        posted_text = first_nonempty_text(card, POSTED_SELS)

        # Keep rows with at least a URL or title
        if not title and not url:
            continue

        rows.append({
            "source": "kijiji",
            "url": url,
            "title": title,
            "price_text": price_text,
            "location_text": loc_text,
            "posted_text": posted_text,
        })
    print("[debug] found", len(cards), "cards with selector section[data-testid='listing-card']")
    return rows

def parse_all_snapshots():
    files = sorted(RAW_DIR.glob("kijiji_search_*.html"))
    if not files:
        raise SystemExit("No raw HTML snapshots found. Run the snapshot step first.")
    all_rows = []
    for f in files:
        html = f.read_text(encoding="utf-8", errors="ignore")
        all_rows.extend(parse_search_html(html))
    df = pd.DataFrame(all_rows).drop_duplicates(subset=["url"]).reset_index(drop=True)

    # Defensive: ensure expected columns exist even if empty
    for col in ["source","url","title","price_text","location_text","posted_text"]:
        if col not in df.columns:
            df[col] = None

    df["scraped_ts"] = datetime.now().isoformat(timespec="seconds")
    out_parquet = PROC_DIR / "listings_raw.parquet"
    try:
        df.to_parquet(out_parquet, index=False)
        return out_parquet
    except Exception:
        out_csv = PROC_DIR / "listings_raw.csv"
        df.to_csv(out_csv, index=False, encoding="utf-8-sig")
        return out_csv
