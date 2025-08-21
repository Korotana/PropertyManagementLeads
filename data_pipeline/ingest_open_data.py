"""
ingest_open_data.py — download the full Saskatoon parcel/address layer
---------------------------------------------------------------------
• Handles the server’s 1 000-record cap per request.
• Stores the result as data/parcels.csv (~77 700 rows).

Run:  python data_pipeline/ingest_open_data.py
"""

import httpx
import pandas as pd
import pathlib
from time import perf_counter

LAYER = "https://gisext.saskatoon.ca/arcgisod/rest/services/OD/LandSurface/MapServer/1"
CSV_PATH = pathlib.Path("data/parcels.csv")

# ------------------------------------------------------------------ #
# 1.  Discover server limits and the OID field name
# ------------------------------------------------------------------ #
def discover_meta():
    meta = httpx.get(LAYER, params={"f": "json"}, timeout=30).json()

    max_rec = meta.get("maxRecordCount", 1000)

    oid_field = (
        meta.get("objectIdField")
        or meta.get("objectIdFieldName")
        or next(
            f["name"] for f in meta["fields"] if f["type"] == "esriFieldTypeOID"
        )
    )
    print(f"ℹ️  maxRecordCount = {max_rec}, OID field = {oid_field}")
    return max_rec, oid_field


# ------------------------------------------------------------------ #
# 2.  Get total row-count (cheap)
# ------------------------------------------------------------------ #
def total_rows():
    r = httpx.get(
        f"{LAYER}/query",
        params={
            "where": "1=1",
            "returnCountOnly": "true",
            "f": "json",
        },
        timeout=30,
    )
    r.raise_for_status()
    count = r.json()["count"]
    print(f"🔢  Layer contains {count:,} rows")
    return count


# ------------------------------------------------------------------ #
# 3.  Pull data in pages using resultOffset + resultRecordCount
# ------------------------------------------------------------------ #
FIELDS = ",".join(
    [
        "SiteId",
        "FullAddress",
        "PostalCode",
        "Ward",
        "Neighbourhood",
        "Zone",
        "SiteArea",
        "LotNumber",
        "BlockNumber",
    ]
)


def fetch_all(max_rec, oid_field, total):
    rows, offset = [], 0
    while offset < total:
        resp = httpx.get(
            f"{LAYER}/query",
            params={
                "where": "1=1",
                "outFields": FIELDS,
                "returnGeometry": "false",
                "f": "json",
                "orderByFields": f"{oid_field} ASC",
                "resultRecordCount": max_rec,
                "resultOffset": offset,
            },
            timeout=60,
        )
        resp.raise_for_status()
        batch = [feat["attributes"] for feat in resp.json()["features"]]
        rows.extend(batch)
        offset += len(batch)                 # advance by actual rows returned
        print(f"📥  {offset:,}/{total:,} downloaded…")
    return pd.DataFrame(rows)


# ------------------------------------------------------------------ #
# 4.  Main entry-point
# ------------------------------------------------------------------ #
if __name__ == "__main__":
    t0 = perf_counter()
    CSV_PATH.parent.mkdir(exist_ok=True)

    max_rec, oid_field = discover_meta()
    total = total_rows()
    df = fetch_all(max_rec, oid_field, total)

    df.to_csv(CSV_PATH, index=False)
    secs = perf_counter() - t0
    print(
        f"✅  Saved {len(df):,} rows → {CSV_PATH.as_posix()} "
        f"({CSV_PATH.stat().st_size/1_048_576:0.1f} MB) in {secs:0.1f}s"
    )
