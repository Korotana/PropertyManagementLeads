# data_pipeline/kijiji_detail.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List
from datetime import datetime
import random
import re

from playwright.sync_api import Page

# --------- Config for on-disk debugging ---------
DETAIL_DUMP_DIR = Path("data/raw/details")
DETAIL_DUMP_DIR.mkdir(parents=True, exist_ok=True)

def dump_detail(page: Page, url: str, idx: int) -> None:
    """
    Save the current detail page HTML + full-page screenshot for inspection.
    """
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    html_path = DETAIL_DUMP_DIR / f"detail_{idx:03d}_{ts}.html"
    png_path  = DETAIL_DUMP_DIR / f"detail_{idx:03d}_{ts}.png"
    html_path.write_text(page.content(), encoding="utf-8")
    try:
        page.screenshot(path=str(png_path), full_page=True)
    except Exception:
        # If full_page fails due to very long page, fall back to viewport
        page.screenshot(path=str(png_path), full_page=False)


# --------- Lightweight helpers ---------
def _text(page: Page, sel: str) -> Optional[str]:
    try:
        t = page.locator(sel).first.inner_text(timeout=1500)
        t = t.strip()
        return t or None
    except Exception:
        return None

def _attr(page: Page, sel: str, attr: str) -> Optional[str]:
    try:
        v = page.locator(sel).first.get_attribute(attr, timeout=1500)
        return v.strip() if v else None
    except Exception:
        return None

def _any_text(page: Page, selectors: List[str]) -> Optional[str]:
    for s in selectors:
        v = _text(page, s)
        if v:
            return v
    return None

def _any_attr(page: Page, selectors: List[str], attr: str) -> Optional[str]:
    for s in selectors:
        v = _attr(page, s, attr)
        if v:
            return v
    return None

def _absolute(href: Optional[str]) -> Optional[str]:
    if not href:
        return None
    return href if href.startswith("http") else f"https://www.kijiji.ca{href}"


# --------- Regex for contact discovery ---------
PHONE_RE = re.compile(
    r"(?:\+?1[\s\-.]?)?(?:\(?\d{3}\)?)[\s\-.]?\d{3}[\s\-.]?\d{4}"
)
EMAIL_RE = re.compile(
    r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"
)


# --------- Result container ---------
@dataclass
class DetailResult:
    url: str
    seller_name: Optional[str]
    seller_profile_url: Optional[str]
    desc: Optional[str]
    posted_iso: Optional[str]
    phone_found: Optional[str]
    email_found: Optional[str]
    address_hint: Optional[str]


# --------- Main scraper for a single listing ---------
def scrape_detail(page: Page, url: str) -> DetailResult:
    """
    Navigate to a Kijiji listing detail page and pull seller & listing info.
    Designed to be resilient to minor DOM changes (multiple selector fallbacks).
    """
    page.goto(url, wait_until="domcontentloaded", timeout=60000)

    # Let lazy content load; gentle scrolls prevent "bot" patterns.
    for _ in range(4):
        page.mouse.wheel(0, 2400)
        page.wait_for_timeout(random.randint(500, 900))

    # --- Seller name ---
    seller_name = _any_text(page, [
        "[data-testid='seller-name']",
        "[data-qa-id='seller-name']",
        "section:has([data-testid='seller']) h2",
        "section:has([data-testid='seller']) [role='heading']",
        "a[aria-label*='seller']",
    ])

    # --- Seller profile link (Other ads) ---
    seller_profile_url = _any_attr(page, [
        "a:has-text(\"View seller's other ads\")",
        "a:has-text('View seller’s other ads')",  # curly apostrophe variant
        "a:has-text('Other ads')",
        "a[href*='/b-other/']",
        "a[href*='/b-same/']",
    ], "href")
    seller_profile_url = _absolute(seller_profile_url)

    # --- Description text ---
    # Kijiji often uses item-description or a VIP area; we try several.
    desc = _any_text(page, [
        "[data-testid='item-description']",
        "section[aria-label*='Description']",
        "section:has(h2:has-text('Description'))",
        "[data-testid='vip-description']",
        "article",
    ])

    # --- Posted timestamp / date ---
    posted_iso = _any_attr(page, ["time[datetime]", "time"], "datetime") or \
                 _any_text(page, ["[data-testid='posted-date']"])

    # --- Location / address hint ---
    address_hint = _any_text(page, [
        "[data-testid='location']",
        "address",
        "section:has([data-testid='location'])",
        "div:has-text('Location') + div",
        "section:has(h2:has-text('Location')) div >> nth=1",
    ])

    # --- Contact discovery (regex over available text) ---
    haystack = " ".join([t for t in [desc, seller_name, address_hint] if t])
    phone_found = None
    email_found = None
    if haystack:
        m = PHONE_RE.search(haystack)
        phone_found = m.group(0) if m else None
        m = EMAIL_RE.search(haystack)
        email_found = m.group(0) if m else None

    return DetailResult(
        url=url,
        seller_name=seller_name,
        seller_profile_url=seller_profile_url,
        desc=desc,
        posted_iso=posted_iso,
        phone_found=phone_found,
        email_found=email_found,
        address_hint=address_hint,
    )
