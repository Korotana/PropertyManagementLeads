# app/main.py
from __future__ import annotations

from pathlib import Path
from datetime import datetime
from time import perf_counter, sleep
from typing import Optional
import random

import pandas as pd
from tqdm import tqdm
from playwright.sync_api import sync_playwright

from data_pipeline.Scrape_kijiji_playwright import snapshot_search_pages
from data_pipeline.scrape_kijiji import parse_all_snapshots
from data_pipeline.utils import extract_beds
from data_pipeline.kijiji_detail import scrape_detail, dump_detail  # ensure dump_detail exists

EXPORTS_DIR = Path("data/exports")
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)


# ---------- Helpers ----------
def simple_property_key(row) -> str:
    """
    Conservative property key using any address hint or the location text.
    Normalizes spacing/case so duplicates collapse.
    """
    base = (row.get("address_hint") or row.get("location_text") or "").lower()
    return " ".join(base.split())


def lead_score(row) -> int:
    """
    Transparent 0–100 lead score.
    Heavier weight on intent & recency, big bonus if direct contact found.
    """
    score = 0
    desc = (row.get("desc") or "").lower()

    # Recency / data presence
    score += 20 if row.get("posted_iso") else 8
    if row.get("seller_name"):
        score += 8
    if row.get("address_hint"):
        score += 6

    # Intent cues
    for kw, pts in [
        ("property management", 25),
        ("long term", 10),
        ("1 year lease", 8),
        ("maintenance", 6),
        ("credit check", 5),
        ("responsible tenant", 4),
    ]:
        if kw in desc:
            score += pts

    # Direct contact (rare on Kijiji, high value)
    if row.get("phone_found"):
        score += 30
    if row.get("email_found"):
        score += 18

    return min(score, 100)


# ---------- Pipeline ----------
def run_pipeline():
    # 1) Snapshot search pages (adjust pages once everything is stable)
    snapshot_search_pages(max_pages=2)

    # 2) Parse cards -> processed file
    parsed_path = parse_all_snapshots()
    df = pd.read_parquet(parsed_path) if str(parsed_path).endswith(".parquet") else pd.read_csv(parsed_path)

    # Defensive: ensure expected cols exist even if parser changes
    for col in ["url", "title", "price_text", "location_text", "posted_text"]:
        if col not in df.columns:
            df[col] = None

    print(f"[info] Parsed {len(df)} listing cards ({df['url'].notna().sum()} with URLs)")
    if df.empty:
        print("Parsed 0 listings — adjust selectors first.")
        return

    # Keep only rows with URLs
    df = df[df["url"].notna()].reset_index(drop=True)
    if df.empty:
        print("No URLs to visit.")
        return

    # 3) Visit details and enrich (with progress & polite jitter)
    urls = df["url"].dropna().tolist()

    # On first run, keep the batch small; remove/comment this line later
    # urls = urls[:20]

    print(f"[info] Visiting {len(urls)} detail pages…")
    details = []

    start = perf_counter()
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"]
        )
        context = browser.new_context(locale="en-CA", viewport={"width": 1280, "height": 900})
        page = context.new_page()

        for i, url in enumerate(tqdm(urls, desc="Scraping detail pages", unit="ad"), 1):
            try:
                d = scrape_detail(page, url)
                # Dump the first few pages for debugging (HTML + PNG)
                if i <= 3:
                    dump_detail(page, url, i)
                details.append(d.__dict__)
            except Exception as e:
                details.append({
                    "url": url,
                    "seller_name": None, "seller_profile_url": None,
                    "desc": None, "posted_iso": None,
                    "phone_found": None, "email_found": None,
                    "address_hint": None, "error": str(e)
                })
            # polite jitter to reduce block risk
            sleep(random.uniform(0.4, 0.9))

        browser.close()

    elapsed = perf_counter() - start
    per = elapsed / max(len(urls), 1)
    print(f"[done] Scraped {len(urls)} detail pages in {elapsed:0.1f}s (~{per:0.1f}s/ad)")

    # 4) Join & transform -> property_key, beds, score
    edf = pd.DataFrame(details)
    out = df.merge(edf, on="url", how="left")

    # Guard against missing columns before transform
    for need in ["title", "location_text"]:
        if need not in out.columns:
            out[need] = None

    out["beds"] = out["title"].map(extract_beds)
    out["property_key"] = out.apply(simple_property_key, axis=1)
    out["lead_score"] = out.apply(lead_score, axis=1)
    out = out.assign(scraped_ts=datetime.now().isoformat(timespec="seconds"))

    # 5) Dedupe per property (keep best-scored)
    lead_cols = [
        "lead_score", "title", "beds", "price_text", "location_text",
        "address_hint", "seller_name", "phone_found", "email_found",
        "url", "seller_profile_url", "posted_iso", "scraped_ts"
    ]
    # Ensure all columns exist for export
    for c in lead_cols:
        if c not in out.columns:
            out[c] = None

    best = (
        out.sort_values(["property_key", "lead_score"], ascending=[True, False])
           .drop_duplicates(subset=["property_key"])
           .loc[:, lead_cols]
    )

    # 6) Export
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    xlsx = EXPORTS_DIR / f"leads_{ts}.xlsx"
    try:
        with pd.ExcelWriter(xlsx, engine="xlsxwriter") as writer:
            best.to_excel(writer, index=False, sheet_name="leads")
        print(f"Exported {len(best)} leads -> {xlsx}")
    except Exception as e:
        # Fallback to CSV if Excel writer isn’t available
        csv = EXPORTS_DIR / f"leads_{ts}.csv"
        best.to_csv(csv, index=False, encoding="utf-8-sig")
        print(f"Exported {len(best)} leads -> {csv} (Excel fallback due to: {e})")


if __name__ == "__main__":
    run_pipeline()
