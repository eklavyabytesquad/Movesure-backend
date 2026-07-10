"""
Party Analytics Service
=======================
Provides consignor / consignee analytics for dashboards and graphs.

Search by GSTIN (preferred) or company name.
All aggregation runs in Python after a single paginated bilty fetch
— avoids PostgREST group-by limitations and the 1 000-row default cap.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Literal

from services.supabase_client import get_supabase

# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

PAGE_SIZE = 1000  # PostgREST default cap; we page through in chunks

PartyType = Literal["consignor", "consignee"]


# ──────────────────────────────────────────────────────────────────────────────
# Date window helpers
# ──────────────────────────────────────────────────────────────────────────────

def _today() -> date:
    return datetime.now(timezone.utc).date()


def _date_windows(anchor: date) -> dict:
    """Return ISO-string date bounds for common analytics windows."""
    year, month = anchor.year, anchor.month

    # This month: 1st of current month → today
    this_month_start = date(year, month, 1)

    # Last month: 1st → last day of prev month
    if month == 1:
        last_month_start = date(year - 1, 12, 1)
        last_month_end   = date(year - 1, 12, 31)
    else:
        last_month_start = date(year, month - 1, 1)
        import calendar
        last_day = calendar.monthrange(year, month - 1)[1]
        last_month_end = date(year, month - 1, last_day)

    # Last 7 days (rolling week)
    last_week_start = anchor - timedelta(days=6)

    # This year
    this_year_start = date(year, 1, 1)

    # Last 6 months for trend
    trend_months = []
    y, m = year, month
    for _ in range(6):
        trend_months.append((y, m))
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    trend_months.reverse()

    return {
        "this_month":  {"from": str(this_month_start), "to": str(anchor)},
        "last_month":  {"from": str(last_month_start), "to": str(last_month_end)},
        "last_week":   {"from": str(last_week_start),  "to": str(anchor)},
        "this_year":   {"from": str(this_year_start),  "to": str(anchor)},
        "_trend_months": trend_months,          # internal, stripped before return
    }


# ──────────────────────────────────────────────────────────────────────────────
# Entity resolver
# ──────────────────────────────────────────────────────────────────────────────

def _resolve_entity(sb, query: str, party_type: PartyType) -> dict | None:
    """
    Resolve a GSTIN or name query to a canonical entity dict.
    Returns: {name, gstin, matched_by}  or None if not found.

    GSTIN resolution: if query looks like a GSTIN (15 uppercase alphanum) check
    consignors / consignees tables. Falls back to bilty rows directly so we
    still return analytics even if the master record doesn't exist.

    Name resolution: ilike match against company_name.
    """
    query = query.strip()
    table = "consignors" if party_type == "consignor" else "consignees"
    gst_col = "gst_num"
    name_col = "company_name"

    # ── GSTIN path ──────────────────────────────────────────────────────────
    is_gstin = len(query) == 15 and query.replace(" ", "").isalnum()
    if is_gstin:
        gst_upper = query.upper()

        # Try master table first
        r = sb.table(table).select(f"id, {name_col}, {gst_col}") \
              .ilike(gst_col, gst_upper).limit(1).execute()
        if r.data:
            row = r.data[0]
            return {
                "id": row["id"],
                "name": row[name_col],
                "gstin": gst_upper,
                "matched_by": "gstin_master",
            }

        # Fallback: confirm GSTIN exists in bilty
        gst_field = "consignor_gst" if party_type == "consignor" else "consignee_gst"
        rb = sb.table("bilty").select(f"consignor_name, consignee_name, {gst_field}") \
               .ilike(gst_field, gst_upper).limit(1).execute()
        if rb.data:
            name_field = "consignor_name" if party_type == "consignor" else "consignee_name"
            return {
                "id": None,
                "name": rb.data[0].get(name_field, ""),
                "gstin": gst_upper,
                "matched_by": "gstin_bilty_fallback",
            }

        return None

    # ── Name path ────────────────────────────────────────────────────────────
    r = sb.table(table).select(f"id, {name_col}, {gst_col}") \
          .ilike(name_col, f"%{query}%").limit(5).execute()
    if not r.data:
        return None

    best = r.data[0]
    return {
        "id": best["id"],
        "name": best[name_col],
        "gstin": (best.get(gst_col) or "").strip().upper() or None,
        "matched_by": "name",
    }


# ──────────────────────────────────────────────────────────────────────────────
# Bilty fetcher (paginated)
# ──────────────────────────────────────────────────────────────────────────────

_BILTY_COLS = (
    "bilty_date, consignor_name, consignor_gst, "
    "consignee_name, consignee_gst, from_city_id, to_city_id, "
    "wt, no_of_pkg, total, payment_mode, delivery_type, branch_id"
)


def _fetch_bilties(sb, entity: dict, party_type: PartyType, date_from: str) -> list[dict]:
    """
    Fetch all active bilty rows for this entity from date_from onward.
    Pages through in PAGE_SIZE chunks to bypass the 1 000-row cap.
    Prefers GST match; falls back to name match if no GST available.
    """
    gst_col  = "consignor_gst"  if party_type == "consignor" else "consignee_gst"
    name_col = "consignor_name" if party_type == "consignor" else "consignee_name"

    rows: list[dict] = []
    offset = 0

    use_gst = bool(entity.get("gstin"))

    while True:
        q = (
            sb.table("bilty")
            .select(_BILTY_COLS)
            .eq("is_active", True)
            .gte("bilty_date", date_from)
            .order("bilty_date", desc=True)
            .range(offset, offset + PAGE_SIZE - 1)
        )

        if use_gst:
            q = q.ilike(gst_col, entity["gstin"])
        else:
            q = q.ilike(name_col, entity["name"])

        resp = q.execute()
        chunk = resp.data or []
        rows.extend(chunk)

        if len(chunk) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    return rows


# ──────────────────────────────────────────────────────────────────────────────
# City lookup
# ──────────────────────────────────────────────────────────────────────────────

def _build_city_map(sb) -> dict[str, str]:
    """Return {city_id: city_name} — whole cities table, small enough to cache."""
    resp = sb.table("cities").select("id, city_name").execute()
    return {r["id"]: r["city_name"] for r in (resp.data or [])}


# ──────────────────────────────────────────────────────────────────────────────
# Aggregation helpers
# ──────────────────────────────────────────────────────────────────────────────

def _empty_bucket() -> dict:
    return {"count": 0, "weight": 0.0, "packages": 0, "value": 0.0}


def _add(bucket: dict, row: dict):
    bucket["count"]    += 1
    bucket["weight"]   += float(row.get("wt")    or 0)
    bucket["packages"] += int(row.get("no_of_pkg") or 0)
    bucket["value"]    += float(row.get("total")  or 0)


def _in_window(bilty_date: str, from_d: str, to_d: str) -> bool:
    return from_d <= bilty_date <= to_d


def _aggregate(rows: list[dict], city_map: dict, entity: dict,
               party_type: PartyType, windows: dict) -> dict:
    """Compute all analytics in a single pass + a few O(n) passes."""

    today      = str(_today())
    tw         = windows["this_month"]
    lw         = windows["last_month"]
    week_w     = windows["last_week"]
    year_w     = windows["this_year"]

    # city key for this party type: consignor sends TO a city; consignee receives FROM
    city_key = "to_city_id" if party_type == "consignor" else "from_city_id"

    # counterparty fields
    cp_name_key = "consignee_name" if party_type == "consignor" else "consignor_name"
    cp_gst_key  = "consignee_gst"  if party_type == "consignor" else "consignor_gst"

    # ── Per-row buckets ────────────────────────────────────────────────────
    summary_tm   = _empty_bucket()
    summary_lm   = _empty_bucket()
    summary_week = _empty_bucket()
    summary_year = _empty_bucket()

    city_tm:  dict[str, dict] = defaultdict(lambda: {**_empty_bucket(), "city_name": ""})
    city_lm:  dict[str, dict] = defaultdict(lambda: {**_empty_bucket(), "city_name": ""})
    city_year: dict[str, dict] = defaultdict(lambda: {**_empty_bucket(), "city_name": ""})

    cp_tm:  dict[str, dict] = defaultdict(lambda: {**_empty_bucket(), "name": "", "gstin": ""})
    cp_lm:  dict[str, dict] = defaultdict(lambda: {**_empty_bucket(), "name": "", "gstin": ""})

    # Monthly trend buckets: {YYYY-MM: bucket}
    trend_buckets: dict[str, dict] = {}
    trend_months  = windows["_trend_months"]
    for y, m in trend_months:
        key = f"{y}-{m:02d}"
        trend_buckets[key] = {**_empty_bucket(), "month": key, "label": date(y, m, 1).strftime("%b %Y")}

    payment_mode_tm: dict[str, int] = defaultdict(int)
    delivery_type_tm: dict[str, int] = defaultdict(int)

    for row in rows:
        bd = (row.get("bilty_date") or "")[:10]
        if not bd:
            continue

        city_id = row.get(city_key) or ""
        city_name = city_map.get(city_id, city_id or "Unknown")

        cp_name = (row.get(cp_name_key) or "").strip()
        cp_gst  = (row.get(cp_gst_key) or "").strip().upper()
        cp_key  = cp_gst or cp_name or "Unknown"

        month_key = bd[:7]

        # ── Summary windows ────────────────────────────────────────────────
        if _in_window(bd, tw["from"], tw["to"]):
            _add(summary_tm, row)

            if city_id:
                _add(city_tm[city_id], row)
                city_tm[city_id]["city_name"] = city_name

            _add(cp_tm[cp_key], row)
            cp_tm[cp_key]["name"]  = cp_name
            cp_tm[cp_key]["gstin"] = cp_gst

            payment_mode_tm[(row.get("payment_mode") or "UNKNOWN").upper()] += 1
            delivery_type_tm[(row.get("delivery_type") or "unknown").lower()] += 1

        if _in_window(bd, lw["from"], lw["to"]):
            _add(summary_lm, row)

            if city_id:
                _add(city_lm[city_id], row)
                city_lm[city_id]["city_name"] = city_name

            _add(cp_lm[cp_key], row)
            cp_lm[cp_key]["name"]  = cp_name
            cp_lm[cp_key]["gstin"] = cp_gst

        if _in_window(bd, week_w["from"], week_w["to"]):
            _add(summary_week, row)

        if _in_window(bd, year_w["from"], year_w["to"]):
            _add(summary_year, row)

            if city_id:
                _add(city_year[city_id], row)
                city_year[city_id]["city_name"] = city_name

        if month_key in trend_buckets:
            _add(trend_buckets[month_key], row)

    # ── Round numerics ──────────────────────────────────────────────────────
    def _round_bucket(b: dict) -> dict:
        return {**b, "weight": round(b["weight"], 2), "value": round(b["value"], 2)}

    # ── City lists (sorted by count desc) ──────────────────────────────────
    def _city_list(d: dict[str, dict], total_count: int) -> list[dict]:
        out = []
        for cid, b in sorted(d.items(), key=lambda x: -x[1]["count"]):
            pct = round(b["count"] / total_count * 100, 1) if total_count else 0
            out.append({
                "city_id":   cid,
                "city_name": b["city_name"],
                "count":     b["count"],
                "weight":    round(b["weight"], 2),
                "packages":  b["packages"],
                "value":     round(b["value"], 2),
                "pct_of_total": pct,
            })
        return out

    city_list_tm   = _city_list(city_tm,   summary_tm["count"])
    city_list_lm   = _city_list(city_lm,   summary_lm["count"])
    city_list_year = _city_list(city_year, summary_year["count"])

    # ── City deltas ─────────────────────────────────────────────────────────
    set_tm_cities = set(city_tm.keys())
    set_lm_cities = set(city_lm.keys())

    new_cities     = [city_map.get(c, c) for c in (set_tm_cities - set_lm_cities)]
    dropped_cities = [city_map.get(c, c) for c in (set_lm_cities - set_tm_cities)]

    declined_cities = []
    for cid in (set_tm_cities & set_lm_cities):
        tm_cnt = city_tm[cid]["count"]
        lm_cnt = city_lm[cid]["count"]
        if tm_cnt < lm_cnt:
            declined_cities.append({
                "city_name":        city_map.get(cid, cid),
                "last_month_count": lm_cnt,
                "this_month_count": tm_cnt,
                "change":           tm_cnt - lm_cnt,
            })
    declined_cities.sort(key=lambda x: x["change"])

    # ── Counterparty lists ──────────────────────────────────────────────────
    def _cp_list(d: dict[str, dict]) -> list[dict]:
        return sorted(
            [
                {
                    "key":      k,
                    "name":     v["name"],
                    "gstin":    v["gstin"],
                    "count":    v["count"],
                    "weight":   round(v["weight"], 2),
                    "packages": v["packages"],
                    "value":    round(v["value"], 2),
                }
                for k, v in d.items()
            ],
            key=lambda x: -x["count"],
        )

    cp_list_tm = _cp_list(cp_tm)
    cp_list_lm = _cp_list(cp_lm)

    # ── Counterparty deltas ─────────────────────────────────────────────────
    set_cp_tm = set(cp_tm.keys())
    set_cp_lm = set(cp_lm.keys())

    new_counterparties = [
        {"name": cp_tm[k]["name"], "gstin": cp_tm[k]["gstin"], "count": cp_tm[k]["count"]}
        for k in (set_cp_tm - set_cp_lm)
    ]
    dropped_counterparties = [
        {"name": cp_lm[k]["name"], "gstin": cp_lm[k]["gstin"], "count": cp_lm[k]["count"]}
        for k in (set_cp_lm - set_cp_tm)
    ]
    recurring_counterparties = [
        {
            "name":             cp_tm[k]["name"],
            "gstin":            cp_tm[k]["gstin"],
            "this_month_count": cp_tm[k]["count"],
            "last_month_count": cp_lm[k]["count"],
            "change":           cp_tm[k]["count"] - cp_lm[k]["count"],
        }
        for k in (set_cp_tm & set_cp_lm)
    ]
    recurring_counterparties.sort(key=lambda x: -x["this_month_count"])

    # ── Monthly trend ───────────────────────────────────────────────────────
    monthly_trend = []
    for y, m in trend_months:
        key = f"{y}-{m:02d}"
        b = trend_buckets[key]
        monthly_trend.append({
            "month":    key,
            "label":    b["label"],
            "count":    b["count"],
            "weight":   round(b["weight"], 2),
            "packages": b["packages"],
            "value":    round(b["value"], 2),
        })

    # ── Payment / delivery mode breakdowns ─────────────────────────────────
    payment_breakdown = [
        {"mode": k, "count": v} for k, v in
        sorted(payment_mode_tm.items(), key=lambda x: -x[1])
    ]
    delivery_breakdown = [
        {"type": k, "count": v} for k, v in
        sorted(delivery_type_tm.items(), key=lambda x: -x[1])
    ]

    # ── Assemble response ───────────────────────────────────────────────────
    return {
        "summary": {
            "this_month":     _round_bucket(summary_tm),
            "last_month":     _round_bucket(summary_lm),
            "last_week":      _round_bucket(summary_week),
            "this_year":      _round_bucket(summary_year),
            "all_time_total": len(rows),
        },
        "monthly_trend": monthly_trend,
        "city_breakdown": {
            "this_month":  city_list_tm,
            "last_month":  city_list_lm,
            "this_year":   city_list_year,
        },
        "city_delta": {
            "new_cities_this_month": sorted(new_cities),
            "dropped_cities":        sorted(dropped_cities),
            "declined_cities":       declined_cities,
        },
        "counterparty_breakdown": {
            "this_month": cp_list_tm,
            "last_month": cp_list_lm,
        },
        "counterparty_delta": {
            "new_this_month":       new_counterparties,
            "dropped":              dropped_counterparties,
            "recurring":            recurring_counterparties,
        },
        "payment_mode_breakdown": {
            "this_month": payment_breakdown,
        },
        "delivery_type_breakdown": {
            "this_month": delivery_breakdown,
        },
    }


# ──────────────────────────────────────────────────────────────────────────────
# Public entry point
# ──────────────────────────────────────────────────────────────────────────────

def get_party_analytics(query: str, party_type: PartyType = "consignor") -> dict:
    """
    Main analytics entry point.

    Parameters
    ----------
    query       : GSTIN (15 chars) or partial company name
    party_type  : "consignor" (default) or "consignee"

    Returns a graph-ready analytics payload.
    """
    sb = get_supabase()
    today = _today()
    windows = _date_windows(today)

    # Fetch data back to start of this year for trend + year summary
    fetch_from = windows["this_year"]["from"]

    # Resolve entity
    entity = _resolve_entity(sb, query, party_type)
    if not entity:
        return {
            "status":  "not_found",
            "message": f"No {party_type} found matching '{query}'",
        }

    # Load city map once
    city_map = _build_city_map(sb)

    # Fetch all relevant bilty rows (paginated)
    rows = _fetch_bilties(sb, entity, party_type, fetch_from)

    # Strip internal keys from windows before returning
    windows_public = {k: v for k, v in windows.items() if not k.startswith("_")}

    analytics = _aggregate(rows, city_map, entity, party_type, windows)

    return {
        "status":     "success",
        "entity":     entity,
        "party_type": party_type,
        "windows":    windows_public,
        "total_rows_fetched": len(rows),
        **analytics,
    }
