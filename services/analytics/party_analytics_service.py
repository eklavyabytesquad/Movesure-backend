"""
Party Analytics Service
=======================
Provides consignor / consignee analytics for dashboards and graphs.

Search by GSTIN (preferred) or company name.
All aggregation runs in Python after a single paginated bilty fetch
— avoids PostgREST group-by limitations and the 1 000-row default cap.

New in v2:
  - 12-month trend (was 6)
  - counterparty_monthly_detail  — per counterparty, 12-month monthly breakdown
  - counterparty_web             — for each top counterparty, how many OTHER
                                   parties they also work with (exclusivity %)
"""

from __future__ import annotations

import calendar
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Literal

from services.supabase_client import get_supabase

# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

PAGE_SIZE   = 1000   # PostgREST row cap — we page in chunks
WEB_TOP_N   = 10     # how many counterparties to fetch web data for

PartyType = Literal["consignor", "consignee"]


# ──────────────────────────────────────────────────────────────────────────────
# Date window helpers
# ──────────────────────────────────────────────────────────────────────────────

def _today() -> date:
    return datetime.now(timezone.utc).date()


def _prev_month(y: int, m: int) -> tuple[int, int]:
    m -= 1
    if m == 0:
        m, y = 12, y - 1
    return y, m


def _month_range(y: int, m: int) -> tuple[str, str]:
    """Return (first_day, last_day) as ISO strings for given year/month."""
    last = calendar.monthrange(y, m)[1]
    return f"{y}-{m:02d}-01", f"{y}-{m:02d}-{last:02d}"


def _build_trend_months(anchor: date, n: int = 12) -> list[tuple[int, int]]:
    y, m = anchor.year, anchor.month
    months = []
    for _ in range(n):
        months.append((y, m))
        y, m = _prev_month(y, m)
    months.reverse()
    return months


def _date_windows(anchor: date) -> dict:
    """Return date bounds for standard analytics windows."""
    year, month = anchor.year, anchor.month

    this_month_start = date(year, month, 1)

    py, pm = _prev_month(year, month)
    last_month_start = date(py, pm, 1)
    last_month_end   = date(py, pm, calendar.monthrange(py, pm)[1])

    last_week_start  = anchor - timedelta(days=6)
    this_year_start  = date(year, 1, 1)

    trend_months_12 = _build_trend_months(anchor, 12)
    # Earliest month in trend is the fetch boundary
    ey, em    = trend_months_12[0]
    fetch_from = f"{ey}-{em:02d}-01"

    return {
        "this_month":     {"from": str(this_month_start), "to": str(anchor)},
        "last_month":     {"from": str(last_month_start), "to": str(last_month_end)},
        "last_week":      {"from": str(last_week_start),  "to": str(anchor)},
        "this_year":      {"from": str(this_year_start),  "to": str(anchor)},
        "_trend_months":  trend_months_12,     # internal — 12 months
        "_fetch_from":    fetch_from,           # internal — earliest date to pull
    }


# ──────────────────────────────────────────────────────────────────────────────
# Entity resolver
# ──────────────────────────────────────────────────────────────────────────────

def _resolve_entity(sb, query: str, party_type: PartyType) -> dict | None:
    """Resolve a GSTIN or name query to a canonical entity dict."""
    query    = query.strip()
    table    = "consignors" if party_type == "consignor" else "consignees"
    gst_col  = "gst_num"
    name_col = "company_name"

    is_gstin = len(query) == 15 and query.replace(" ", "").isalnum()
    if is_gstin:
        gst_upper = query.upper()

        r = sb.table(table).select(f"id, {name_col}, {gst_col}") \
              .ilike(gst_col, gst_upper).limit(1).execute()
        if r.data:
            row = r.data[0]
            return {"id": row["id"], "name": row[name_col], "gstin": gst_upper, "matched_by": "gstin_master"}

        gst_field = "consignor_gst" if party_type == "consignor" else "consignee_gst"
        rb = sb.table("bilty").select(f"consignor_name, consignee_name, {gst_field}") \
               .ilike(gst_field, gst_upper).limit(1).execute()
        if rb.data:
            name_field = "consignor_name" if party_type == "consignor" else "consignee_name"
            return {"id": None, "name": rb.data[0].get(name_field, ""), "gstin": gst_upper, "matched_by": "gstin_bilty_fallback"}

        return None

    r = sb.table(table).select(f"id, {name_col}, {gst_col}") \
          .ilike(name_col, f"%{query}%").limit(5).execute()
    if not r.data:
        return None

    best = r.data[0]
    return {
        "id":         best["id"],
        "name":       best[name_col],
        "gstin":      (best.get(gst_col) or "").strip().upper() or None,
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

_WEB_COLS = (
    "bilty_date, consignor_name, consignor_gst, "
    "consignee_name, consignee_gst, wt, no_of_pkg, total"
)


def _paginated_fetch(sb, gst_col: str, gst_val: str,
                     name_col: str, name_val: str,
                     date_from: str, select_cols: str) -> list[dict]:
    """Generic paginated bilty fetch — prefer GST match, fall back to name."""
    rows: list[dict] = []
    offset = 0
    use_gst = bool(gst_val)

    while True:
        q = (
            sb.table("bilty")
            .select(select_cols)
            .eq("is_active", True)
            .gte("bilty_date", date_from)
            .order("bilty_date", desc=True)
            .range(offset, offset + PAGE_SIZE - 1)
        )
        q = q.ilike(gst_col, gst_val) if use_gst else q.ilike(name_col, name_val)

        chunk = (q.execute().data or [])
        rows.extend(chunk)
        if len(chunk) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    return rows


def _fetch_bilties(sb, entity: dict, party_type: PartyType, date_from: str) -> list[dict]:
    gst_col  = "consignor_gst"  if party_type == "consignor" else "consignee_gst"
    name_col = "consignor_name" if party_type == "consignor" else "consignee_name"
    return _paginated_fetch(
        sb,
        gst_col,  entity.get("gstin") or "",
        name_col, entity.get("name")  or "",
        date_from, _BILTY_COLS,
    )


# ──────────────────────────────────────────────────────────────────────────────
# City lookup
# ──────────────────────────────────────────────────────────────────────────────

def _build_city_map(sb) -> dict[str, str]:
    resp = sb.table("cities").select("id, city_name").execute()
    return {r["id"]: r["city_name"] for r in (resp.data or [])}


# ──────────────────────────────────────────────────────────────────────────────
# Aggregation helpers
# ──────────────────────────────────────────────────────────────────────────────

def _empty_bucket() -> dict:
    return {"count": 0, "weight": 0.0, "packages": 0, "value": 0.0}


def _add(bucket: dict, row: dict):
    bucket["count"]    += 1
    bucket["weight"]   += float(row.get("wt")        or 0)
    bucket["packages"] += int(row.get("no_of_pkg")   or 0)
    bucket["value"]    += float(row.get("total")     or 0)


def _round_bucket(b: dict) -> dict:
    return {**b, "weight": round(b["weight"], 2), "value": round(b["value"], 2)}


def _in_window(bd: str, from_d: str, to_d: str) -> bool:
    return from_d <= bd <= to_d


# ──────────────────────────────────────────────────────────────────────────────
# Counterparty web fetch
# ──────────────────────────────────────────────────────────────────────────────

def _fetch_counterparty_web(
    sb,
    top_cps: list[dict],
    subject_entity: dict,
    party_type: PartyType,
    date_from: str,
    trend_months: list[tuple[int, int]],
) -> list[dict]:
    """
    For each of the top N counterparties:
      1. Fetch ALL their bilties over 12 months (not filtered to subject).
      2. Compute how many DISTINCT other parties they work with.
      3. Calculate exclusivity % = subject's share of their total volume.
      4. Return per-month total activity for the web graph.

    party_type = the subject's type.
    If subject is "consignor", counterparties are "consignee" — so we query
    bilty by consignee_gst and look at the consignor_gst distribution.
    """
    # For the counterparty's perspective:
    #   their own column  = opposite of subject
    #   partner column    = subject's column (to count distinct partners)
    if party_type == "consignor":
        cp_gst_col   = "consignee_gst"
        cp_name_col  = "consignee_name"
        partner_gst  = "consignor_gst"
        partner_name = "consignor_name"
    else:
        cp_gst_col   = "consignor_gst"
        cp_name_col  = "consignor_name"
        partner_gst  = "consignee_gst"
        partner_name = "consignee_name"

    subject_gstin = subject_entity.get("gstin") or ""
    subject_name  = subject_entity.get("name")  or ""

    # Build month key set for fast lookup
    month_keys = [f"{y}-{m:02d}" for y, m in trend_months]

    web_result = []

    for cp in top_cps[:WEB_TOP_N]:
        cp_gstin = cp.get("gstin") or ""
        cp_name  = cp.get("name")  or ""

        if not cp_gstin and not cp_name:
            continue

        # Fetch all bilties where this counterparty appears (any partner)
        all_rows = _paginated_fetch(
            sb,
            cp_gst_col,  cp_gstin,
            cp_name_col, cp_name,
            date_from, _WEB_COLS,
        )

        if not all_rows:
            continue

        # ── Aggregations ─────────────────────────────────────────────────────
        total_all      = _empty_bucket()
        total_with_subj = _empty_bucket()

        # distinct partners: {partner_key: {name, gstin, count}}
        partners: dict[str, dict] = defaultdict(lambda: {"name": "", "gstin": "", "count": 0})

        # monthly total (all partners, including subject)
        monthly_all: dict[str, dict] = {mk: {**_empty_bucket(), "month": mk} for mk in month_keys}

        # monthly with subject only
        monthly_with_subj: dict[str, dict] = {mk: {**_empty_bucket(), "month": mk} for mk in month_keys}

        for row in all_rows:
            bd = (row.get("bilty_date") or "")[:10]
            if not bd:
                continue

            pg  = (row.get(partner_gst)  or "").strip().upper()
            pn  = (row.get(partner_name) or "").strip()
            pk  = pg or pn or "UNKNOWN"

            is_subject = (
                (subject_gstin and pg == subject_gstin) or
                (not subject_gstin and pn.upper() == subject_name.upper())
            )

            _add(total_all, row)

            partners[pk]["name"]   = pn
            partners[pk]["gstin"]  = pg
            partners[pk]["count"] += 1

            mk = bd[:7]
            if mk in monthly_all:
                _add(monthly_all[mk], row)

            if is_subject:
                _add(total_with_subj, row)
                if mk in monthly_with_subj:
                    _add(monthly_with_subj[mk], row)

        # Distinct partner count (excluding subject itself)
        all_partner_keys = set(partners.keys())
        subject_key = subject_gstin or subject_name.upper() or ""
        other_partners = {k: v for k, v in partners.items()
                          if k != subject_key and k != "UNKNOWN"}

        # Top 5 OTHER partners (excluding subject)
        top_other = sorted(other_partners.values(), key=lambda x: -x["count"])[:5]

        total_cnt = total_all["count"] or 1
        exclusivity_pct = round(total_with_subj["count"] / total_cnt * 100, 1)

        # Build monthly series
        monthly_series = []
        for mk in month_keys:
            ta = monthly_all[mk]
            ts = monthly_with_subj[mk]
            monthly_series.append({
                "month":              mk,
                "label":              date(int(mk[:4]), int(mk[5:7]), 1).strftime("%b %Y"),
                # Total activity of this counterparty (all partners)
                "total_count":        ta["count"],
                "total_weight":       round(ta["weight"], 2),
                "total_packages":     ta["packages"],
                "total_value":        round(ta["value"], 2),
                # Their activity specifically with the subject
                "with_subject_count":    ts["count"],
                "with_subject_weight":   round(ts["weight"], 2),
                "with_subject_packages": ts["packages"],
                "with_subject_value":    round(ts["value"], 2),
            })

        web_result.append({
            "name":                  cp_name,
            "gstin":                 cp_gstin,
            # Subject's share
            "consignments_with_subject_12m": total_with_subj["count"],
            "weight_with_subject_12m":       round(total_with_subj["weight"], 2),
            "packages_with_subject_12m":     total_with_subj["packages"],
            "value_with_subject_12m":        round(total_with_subj["value"], 2),
            # Their total footprint
            "total_consignments_12m":        total_all["count"],
            "total_distinct_partners":       len(all_partner_keys) - (1 if subject_key in all_partner_keys else 0),
            "exclusivity_pct":               exclusivity_pct,
            # Other partners they work with
            "other_top_partners":            top_other,
            # Monthly breakdown — 12 bars for graph
            "monthly_activity":              monthly_series,
        })

    return web_result


# ──────────────────────────────────────────────────────────────────────────────
# Main aggregation
# ──────────────────────────────────────────────────────────────────────────────

def _aggregate(
    rows: list[dict],
    city_map: dict,
    entity: dict,
    party_type: PartyType,
    windows: dict,
) -> dict:
    """Compute all analytics in Python from the pre-fetched bilty rows."""

    tw      = windows["this_month"]
    lw      = windows["last_month"]
    week_w  = windows["last_week"]
    year_w  = windows["this_year"]
    trend_months = windows["_trend_months"]  # 12-month list of (y, m)

    city_key    = "to_city_id"    if party_type == "consignor" else "from_city_id"
    cp_name_key = "consignee_name" if party_type == "consignor" else "consignor_name"
    cp_gst_key  = "consignee_gst"  if party_type == "consignor" else "consignor_gst"

    # ── Summary buckets ────────────────────────────────────────────────────────
    summary_tm   = _empty_bucket()
    summary_lm   = _empty_bucket()
    summary_week = _empty_bucket()
    summary_year = _empty_bucket()

    # ── City buckets ───────────────────────────────────────────────────────────
    city_tm:   dict[str, dict] = defaultdict(lambda: {**_empty_bucket(), "city_name": ""})
    city_lm:   dict[str, dict] = defaultdict(lambda: {**_empty_bucket(), "city_name": ""})
    city_year: dict[str, dict] = defaultdict(lambda: {**_empty_bucket(), "city_name": ""})

    # ── Counterparty buckets ───────────────────────────────────────────────────
    # this month / last month (for delta)
    cp_tm: dict[str, dict] = defaultdict(lambda: {**_empty_bucket(), "name": "", "gstin": ""})
    cp_lm: dict[str, dict] = defaultdict(lambda: {**_empty_bucket(), "name": "", "gstin": ""})

    # all-time within 12m window (for monthly detail + web)
    cp_all: dict[str, dict] = defaultdict(lambda: {**_empty_bucket(), "name": "", "gstin": ""})

    # per-counterparty monthly breakdown: {cp_key: {month_key: bucket}}
    cp_monthly: dict[str, dict[str, dict]] = defaultdict(
        lambda: defaultdict(_empty_bucket)
    )

    # ── Trend buckets ──────────────────────────────────────────────────────────
    trend_buckets: dict[str, dict] = {}
    for y, m in trend_months:
        key = f"{y}-{m:02d}"
        trend_buckets[key] = {**_empty_bucket(), "month": key, "label": date(y, m, 1).strftime("%b %Y")}

    # ── Payment / delivery ─────────────────────────────────────────────────────
    payment_mode_tm: dict[str, int]  = defaultdict(int)
    delivery_type_tm: dict[str, int] = defaultdict(int)

    # ── Single pass ────────────────────────────────────────────────────────────
    for row in rows:
        bd = (row.get("bilty_date") or "")[:10]
        if not bd:
            continue

        city_id   = row.get(city_key) or ""
        city_name = city_map.get(city_id, city_id or "Unknown")

        cp_name  = (row.get(cp_name_key) or "").strip()
        cp_gst   = (row.get(cp_gst_key)  or "").strip().upper()
        cp_key   = cp_gst or cp_name or "Unknown"

        month_key = bd[:7]

        # Counterparty all-time (12m)
        _add(cp_all[cp_key], row)
        cp_all[cp_key]["name"]  = cp_name
        cp_all[cp_key]["gstin"] = cp_gst

        # Counterparty monthly detail
        _add(cp_monthly[cp_key][month_key], row)

        # 12-month trend bucket
        if month_key in trend_buckets:
            _add(trend_buckets[month_key], row)

        # This month
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

        # Last month
        if _in_window(bd, lw["from"], lw["to"]):
            _add(summary_lm, row)
            if city_id:
                _add(city_lm[city_id], row)
                city_lm[city_id]["city_name"] = city_name
            _add(cp_lm[cp_key], row)
            cp_lm[cp_key]["name"]  = cp_name
            cp_lm[cp_key]["gstin"] = cp_gst

        # Last week
        if _in_window(bd, week_w["from"], week_w["to"]):
            _add(summary_week, row)

        # This year
        if _in_window(bd, year_w["from"], year_w["to"]):
            _add(summary_year, row)
            if city_id:
                _add(city_year[city_id], row)
                city_year[city_id]["city_name"] = city_name

    # ── City lists ─────────────────────────────────────────────────────────────
    def _city_list(d: dict, total_count: int) -> list[dict]:
        out = []
        for cid, b in sorted(d.items(), key=lambda x: -x[1]["count"]):
            pct = round(b["count"] / total_count * 100, 1) if total_count else 0
            out.append({
                "city_id":      cid,
                "city_name":    b["city_name"],
                "count":        b["count"],
                "weight":       round(b["weight"], 2),
                "packages":     b["packages"],
                "value":        round(b["value"], 2),
                "pct_of_total": pct,
            })
        return out

    city_list_tm   = _city_list(city_tm,   summary_tm["count"])
    city_list_lm   = _city_list(city_lm,   summary_lm["count"])
    city_list_year = _city_list(city_year, summary_year["count"])

    # ── City deltas ─────────────────────────────────────────────────────────────
    set_tm_cities = set(city_tm.keys())
    set_lm_cities = set(city_lm.keys())
    new_cities     = sorted([city_map.get(c, c) for c in (set_tm_cities - set_lm_cities)])
    dropped_cities = sorted([city_map.get(c, c) for c in (set_lm_cities - set_tm_cities)])
    declined_cities = sorted(
        [
            {
                "city_name":        city_map.get(cid, cid),
                "last_month_count": city_lm[cid]["count"],
                "this_month_count": city_tm[cid]["count"],
                "change":           city_tm[cid]["count"] - city_lm[cid]["count"],
            }
            for cid in (set_tm_cities & set_lm_cities)
            if city_tm[cid]["count"] < city_lm[cid]["count"]
        ],
        key=lambda x: x["change"],
    )

    # ── Counterparty breakdown (this / last month) ─────────────────────────────
    def _cp_list(d: dict) -> list[dict]:
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

    # ── Counterparty deltas ────────────────────────────────────────────────────
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
    recurring_counterparties = sorted(
        [
            {
                "name":             cp_tm[k]["name"],
                "gstin":            cp_tm[k]["gstin"],
                "this_month_count": cp_tm[k]["count"],
                "last_month_count": cp_lm[k]["count"],
                "change":           cp_tm[k]["count"] - cp_lm[k]["count"],
            }
            for k in (set_cp_tm & set_cp_lm)
        ],
        key=lambda x: -x["this_month_count"],
    )

    # ── Counterparty monthly detail (12-month per counterparty) ────────────────
    month_keys = [f"{y}-{m:02d}" for y, m in trend_months]

    counterparty_monthly_detail = []
    # Sort by total 12m count, take top 15
    top_cp_all = sorted(cp_all.items(), key=lambda x: -x[1]["count"])[:15]
    for cp_key, cp_data in top_cp_all:
        cp_months = cp_monthly[cp_key]

        monthly_series = []
        active_months  = 0
        first_seen     = None
        last_seen      = None

        for mk in month_keys:
            b = cp_months.get(mk, _empty_bucket())
            monthly_series.append({
                "month":    mk,
                "label":    date(int(mk[:4]), int(mk[5:7]), 1).strftime("%b %Y"),
                "count":    b["count"],
                "weight":   round(b["weight"], 2),
                "packages": b["packages"],
                "value":    round(b["value"], 2),
            })
            if b["count"] > 0:
                active_months += 1
                if first_seen is None:
                    first_seen = mk
                last_seen = mk

        counterparty_monthly_detail.append({
            "key":           cp_key,
            "name":          cp_data["name"],
            "gstin":         cp_data["gstin"],
            "total_12m":     _round_bucket(cp_data),
            "active_months": active_months,
            "first_seen":    first_seen,
            "last_seen":     last_seen,
            "monthly":       monthly_series,
        })

    # ── 12-month overall trend ─────────────────────────────────────────────────
    monthly_trend = []
    for y, m in trend_months:
        key = f"{y}-{m:02d}"
        b   = trend_buckets[key]
        monthly_trend.append({
            "month":    key,
            "label":    b["label"],
            "count":    b["count"],
            "weight":   round(b["weight"], 2),
            "packages": b["packages"],
            "value":    round(b["value"], 2),
        })

    # ── Payment / delivery mode ────────────────────────────────────────────────
    payment_breakdown = sorted(
        [{"mode": k, "count": v} for k, v in payment_mode_tm.items()],
        key=lambda x: -x["count"],
    )
    delivery_breakdown = sorted(
        [{"type": k, "count": v} for k, v in delivery_type_tm.items()],
        key=lambda x: -x["count"],
    )

    # Top counterparties for web fetch (by 12m total, with gstin or name)
    top_for_web = [
        {"name": v["name"], "gstin": v["gstin"]}
        for _, v in top_cp_all[:WEB_TOP_N]
    ]

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
            "this_month": city_list_tm,
            "last_month": city_list_lm,
            "this_year":  city_list_year,
        },
        "city_delta": {
            "new_cities_this_month": new_cities,
            "dropped_cities":        dropped_cities,
            "declined_cities":       declined_cities,
        },
        "counterparty_breakdown": {
            "this_month": cp_list_tm,
            "last_month": cp_list_lm,
        },
        "counterparty_delta": {
            "new_this_month": new_counterparties,
            "dropped":        dropped_counterparties,
            "recurring":      recurring_counterparties,
        },
        "counterparty_monthly_detail": counterparty_monthly_detail,
        "payment_mode_breakdown": {
            "this_month": payment_breakdown,
        },
        "delivery_type_breakdown": {
            "this_month": delivery_breakdown,
        },
        "_top_for_web": top_for_web,   # internal, used below
    }


# ──────────────────────────────────────────────────────────────────────────────
# Public entry point
# ──────────────────────────────────────────────────────────────────────────────

def get_party_analytics(query: str, party_type: PartyType = "consignor") -> dict:
    """
    Main analytics entry point.

    Parameters
    ----------
    query      : GSTIN (15 chars) or partial company name
    party_type : "consignor" (default) or "consignee"
    """
    sb    = get_supabase()
    today = _today()
    windows = _date_windows(today)

    # 12-month fetch window
    fetch_from = windows["_fetch_from"]

    # Resolve entity
    entity = _resolve_entity(sb, query, party_type)
    if not entity:
        return {"status": "not_found", "message": f"No {party_type} found matching '{query}'"}

    # City map (small table, load once)
    city_map = _build_city_map(sb)

    # Fetch all bilties for this entity (paginated, 12 months)
    rows = _fetch_bilties(sb, entity, party_type, fetch_from)

    # Aggregate everything from the main rows
    analytics = _aggregate(rows, city_map, entity, party_type, windows)

    # Counterparty web — additional queries per top counterparty
    top_for_web = analytics.pop("_top_for_web", [])
    counterparty_web = _fetch_counterparty_web(
        sb, top_for_web, entity, party_type,
        fetch_from, windows["_trend_months"],
    )

    windows_public = {k: v for k, v in windows.items() if not k.startswith("_")}

    return {
        "status":              "success",
        "entity":              entity,
        "party_type":          party_type,
        "windows":             windows_public,
        "total_rows_fetched":  len(rows),
        **analytics,
        "counterparty_web":    counterparty_web,
    }
