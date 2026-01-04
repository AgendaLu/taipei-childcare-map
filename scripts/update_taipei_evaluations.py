#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Update Taipei daycare center evaluation grades from Taipei Open Data API.

- Fetches dataset: https://data.taipei/api/v1/dataset/43c0bdc5-ddb0-40bf-accd-a096d8c5ac23?scope=resourceAquire
- Merges yearly grades (e.g., 110年..114年) into your existing map centers (by 序號/id).
- Keeps "historical" results by year in evaluation_by_year.
- Writes ./data.json which your GitHub Pages can load.

Repo expectation (recommended):
- ./臺北市準公共化托嬰中心.csv   (base center list, has 序號/機構名稱/地址/電話... and maybe 評鑑結果 like 111-乙)
- ./XY Data.csv                  (geocoded points, has id/Response_Address/lat/lng)
- ./data.json                    (output)

Scheduling:
- Designed to be run daily by GitHub Actions; it will only update once every 10 days
  unless FORCE_UPDATE=1 is set.
"""
from __future__ import annotations

import csv
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests


API_BASE = "https://data.taipei/api/v1/dataset/43c0bdc5-ddb0-40bf-accd-a096d8c5ac23?scope=resourceAquire"
OUTPUT_JSON = "data.json"
CENTERS_CSV = "臺北市準公共化托嬰中心.csv"
XY_CSV = "XY Data.csv"

UPDATE_INTERVAL_DAYS = 10
DEFAULT_LIMIT = 1000  # dataset count is small (~hundreds)


YEAR_COL_RE = re.compile(r"^(?P<yr>\d{3})年$")          # e.g. "114年"
LEGACY_EVAL_RE = re.compile(r"^(?P<yr>\d{3})\s*[-－]\s*(?P<grade>.+?)\s*$")  # e.g. "111-乙"


def now_taipei_iso() -> str:
    # GitHub runner is UTC. Use fixed offset +08:00 for Taipei.
    tz = timezone(timedelta(hours=8))
    return datetime.now(tz=tz).replace(microsecond=0).isoformat()


def safe_int(x: Any) -> Optional[int]:
    try:
        return int(str(x).strip())
    except Exception:
        return None


def load_csv(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return [dict(r) for r in reader]


def fetch_all_records() -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Fetch all records using limit/offset paging."""
    out: List[Dict[str, Any]] = []
    offset = 0
    meta: Dict[str, Any] = {}
    session = requests.Session()
    session.headers.update({"User-Agent": "taipei-daycare-map/1.0 (+github-actions)"})

    while True:
        url = f"{API_BASE}&limit={DEFAULT_LIMIT}&offset={offset}"
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
        payload = resp.json()
        result = payload.get("result", {})
        if not meta:
            meta = {
                "api_url": API_BASE,
                "reported_count": result.get("count"),
                "reported_limit": result.get("limit"),
                "reported_offset": result.get("offset"),
            }

        records = result.get("results", []) or []
        out.extend(records)

        count = result.get("count", len(out))
        if len(out) >= int(count):
            break
        offset += DEFAULT_LIMIT

    return out, meta


def parse_eval_years(rec: Dict[str, Any]) -> Dict[str, str]:
    """Extract year->grade from keys like '110年'.."""
    years: Dict[str, str] = {}
    for k, v in rec.items():
        m = YEAR_COL_RE.match(str(k).strip())
        if not m:
            continue
        yr = m.group("yr")
        grade = "" if v is None else str(v).strip()
        years[yr] = grade
    return years


def normalize_lat_lng(lat: Optional[float], lng: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    """Fix common swap: if lat looks like 121.x and lng looks like 25.x, swap."""
    if lat is None or lng is None:
        return lat, lng
    try:
        if abs(lat) > 90 and abs(lng) <= 90:
            return lng, lat
    except Exception:
        pass
    return lat, lng


def load_existing_json(path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def should_run_update(existing: Optional[Dict[str, Any]]) -> bool:
    if os.getenv("FORCE_UPDATE", "").strip() == "1":
        return True
    if not existing:
        return True
    meta = existing.get("meta", {})
    last = meta.get("last_successful_update")
    if not last:
        return True
    try:
        # accept both "2026-01-04T..." and "2026-01-04 ..."
        last_dt = datetime.fromisoformat(str(last).replace(" ", "T"))
    except Exception:
        return True

    # Compare in Taipei time (fixed +08:00)
    tz = timezone(timedelta(hours=8))
    now_dt = datetime.now(tz=tz)
    if last_dt.tzinfo is None:
        last_dt = last_dt.replace(tzinfo=tz)

    return (now_dt - last_dt) >= timedelta(days=UPDATE_INTERVAL_DAYS)


def main() -> int:
    existing = load_existing_json(OUTPUT_JSON)
    if not should_run_update(existing):
        print(f"Skip: last update < {UPDATE_INTERVAL_DAYS} days. (Set FORCE_UPDATE=1 to bypass)")
        return 0

    # Base centers list (your map dataset)
    if not os.path.exists(CENTERS_CSV):
        print(f"ERROR: missing {CENTERS_CSV} in repo root.")
        return 2
    centers = load_csv(CENTERS_CSV)

    xy_map: Dict[int, Dict[str, Any]] = {}
    if os.path.exists(XY_CSV):
        for r in load_csv(XY_CSV):
            cid = safe_int(r.get("id"))
            if not cid:
                continue
            try:
                lat = float(r.get("lat", "") or "nan")
                lng = float(r.get("lng", "") or "nan")
            except Exception:
                lat, lng = None, None
            if lat is not None and (str(lat) == "nan"):
                lat = None
            if lng is not None and (str(lng) == "nan"):
                lng = None
            lat, lng = normalize_lat_lng(lat, lng)
            xy_map[cid] = {
                "response_address": (r.get("Response_Address") or "").strip(),
                "lat": lat,
                "lng": lng,
            }

    # Fetch evaluations from API
    records, api_meta = fetch_all_records()

    # Index API by 編號 (string)
    api_by_no: Dict[int, Dict[str, Any]] = {}
    api_by_name: Dict[str, Dict[str, Any]] = {}
    for rec in records:
        no = safe_int(rec.get("編號"))
        if no is not None:
            api_by_no[no] = rec
        name = str(rec.get("機構名稱") or "").strip()
        if name:
            api_by_name[name] = rec

    out_centers: List[Dict[str, Any]] = []
    unmatched: List[int] = []

    for c in centers:
        cid = safe_int(c.get("序號"))
        if cid is None:
            continue

        name = (c.get("機構名稱") or "").strip()
        district = (c.get("行政區") or "").strip()
        address = (c.get("地址") or "").strip()
        phone = (c.get("電話") or "").strip()
        cap = (c.get("核定收托人數") or "").strip()
        actual = (c.get("實際收托人數") or "").strip()

        # Start evaluation_by_year with anything we already have from existing data.json
        eval_by_year: Dict[str, str] = {}
        if existing:
            for old in existing.get("centers", []):
                if safe_int(old.get("id")) == cid:
                    eval_by_year.update(old.get("evaluation_by_year", {}) or {})
                    break

        # Merge legacy single field like "111-乙" from your CSV if present
        legacy = (c.get("評鑑結果") or "").strip()
        m = LEGACY_EVAL_RE.match(legacy)
        if m:
            eval_by_year[m.group("yr")] = m.group("grade").strip()

        # Merge yearly fields from API dataset
        rec = api_by_no.get(cid) or api_by_name.get(name)
        source_importdate = None
        if rec:
            eval_by_year.update(parse_eval_years(rec))
            imp = rec.get("_importdate", {})
            # sample: {"date":"2025-12-15 10:17:58.487468","timezone":"Asia/Taipei",...}
            if isinstance(imp, dict) and imp.get("date"):
                source_importdate = str(imp.get("date"))
        else:
            unmatched.append(cid)

        # Attach geocodes (if any)
        geo = xy_map.get(cid, {})
        lat = geo.get("lat")
        lng = geo.get("lng")

        out_centers.append({
            "id": cid,
            "name": name,
            "district": district,
            "address": address,
            "phone": phone,
            "capacity_approved": cap,
            "capacity_current": actual,
            "response_address": geo.get("response_address"),
            "lat": lat,
            "lng": lng,
            "evaluation_by_year": dict(sorted(eval_by_year.items(), key=lambda kv: kv[0])),
            "source_importdate": source_importdate,
        })

    out = {
        "meta": {
            "source": API_BASE,
            "fetched_at": now_taipei_iso(),
            "last_successful_update": now_taipei_iso(),
            "update_interval_days": UPDATE_INTERVAL_DAYS,
            "api_reported_count": api_meta.get("reported_count"),
            "notes": "evaluation_by_year uses Minguo year as string keys, e.g. '114'. Values like '優/甲/乙/丙/已歇業/...' are kept as-is.",
            "unmatched_center_ids_in_base_list": unmatched[:50],  # avoid huge meta; see logs for full
            "unmatched_count": len(unmatched),
        },
        "centers": out_centers,
    }

    # Write only if changed to reduce noise
    new_json = json.dumps(out, ensure_ascii=False, indent=2, sort_keys=False)
    old_json = None
    if os.path.exists(OUTPUT_JSON):
        old_json = open(OUTPUT_JSON, "r", encoding="utf-8").read()

    if old_json == new_json:
        print("No change in data.json")
        return 0

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        f.write(new_json)
        f.write("\n")
    print(f"Updated {OUTPUT_JSON}: centers={len(out_centers)}, api_records={len(records)}, unmatched={len(unmatched)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
