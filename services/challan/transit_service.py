"""
Transit Service
Manages bilty ↔ challan assignments (transit_details).
- Get available bilties (not in any challan)
- Add bilties to transit (with dispatch-lock validation)
- Remove bilties from transit
- Bulk delivery status updates
"""
from datetime import datetime, timezone
from services.supabase_client import get_supabase

TRANSIT_COLS = (
    "id, challan_no, gr_no, bilty_id, challan_book_id, "
    "from_branch_id, to_branch_id, "
    "is_out_of_delivery_from_branch1, out_of_delivery_from_branch1_date, "
    "is_delivered_at_branch2, delivered_at_branch2_date, "
    "is_out_of_delivery_from_branch2, out_of_delivery_from_branch2_date, "
    "is_delivered_at_destination, delivered_at_destination_date, "
    "out_for_door_delivery, out_for_door_delivery_date, "
    "delivery_agent_name, delivery_agent_phone, vehicle_number, "
    "remarks, created_by, created_at, updated_at"
)

PAGE_SIZE = 50


def _now():
    return datetime.now(timezone.utc).isoformat()


# ── AVAILABLE BILTIES (via Supabase RPC) ──────────────────────

def get_available_bilties(page: int = 1, page_size: int = PAGE_SIZE,
                          search: str = None, payment_mode: str = None,
                          city_id: str = None, source: str = None,
                          branch_id: str = None) -> dict:
    """
    Get bilties NOT assigned to any active transit.
    Uses Supabase RPC `get_available_gr_numbers` for the heavy NOT-EXISTS
    check, then fetches full details only for the current page of results.
    """
    try:
        sb = get_supabase()

        # 1. Call RPC — single DB query does the NOT-EXISTS join
        all_available = sb.rpc("get_available_gr_numbers", {
            "p_limit": 100000,
            "p_offset": 0,
        }).execute()
        rows = all_available.data or []

        # 2. Apply source filter
        if source == "bilty":
            rows = [r for r in rows if r["source_table"] == "bilty"]
        elif source == "station":
            rows = [r for r in rows if r["source_table"] == "station_bilty_summary"]

        # Build GR→source map for later
        gr_source = {r["gr_no"]: r["source_table"] for r in rows}
        bilty_grs = [gr for gr, src in gr_source.items() if src == "bilty"]
        station_grs = [gr for gr, src in gr_source.items() if src == "station_bilty_summary"]

        # 3. Fetch full details for bilty GRs
        bilty_detail_map = {}
        if bilty_grs:
            for i in range(0, len(bilty_grs), 500):
                chunk = bilty_grs[i:i + 500]
                bq = sb.table("bilty").select(
                    "id, gr_no, branch_id, bilty_date, delivery_type, "
                    "consignor_name, consignor_gst, consignor_number, "
                    "consignee_name, consignee_gst, consignee_number, "
                    "transport_name, transport_gst, transport_number, transport_id, "
                    "payment_mode, no_of_pkg, wt, rate, freight_amount, "
                    "labour_charge, bill_charge, toll_charge, dd_charge, other_charge, pf_charge, total, "
                    "from_city_id, to_city_id, e_way_bill, pvt_marks, remark, "
                    "saving_option, is_active"
                ).in_("gr_no", chunk).eq("is_active", True).execute()
                for b in (bq.data or []):
                    if b.get("consignor_name") != "CANCEL BILTY":
                        b["source_table"] = "bilty"
                        bilty_detail_map[b["gr_no"]] = b

        # 4. Fetch full details for station GRs
        station_detail_map = {}
        if station_grs:
            for i in range(0, len(station_grs), 500):
                chunk = station_grs[i:i + 500]
                sq = sb.table("station_bilty_summary").select(
                    "id, gr_no, station, consignor, consignee, contents, "
                    "no_of_packets, weight, payment_status, amount, pvt_marks, "
                    "delivery_type, staff_id, branch_id, e_way_bill, "
                    "transport_id, transport_name, transport_gst, city_id, "
                    "created_at, updated_at"
                ).in_("gr_no", chunk).execute()
                for s in (sq.data or []):
                    s["source_table"] = "station_bilty_summary"
                    station_detail_map[s["gr_no"]] = s

        # 5. Merge: build final list preserving RPC order (sorted by gr_no)
        merged = []
        for r in rows:
            gr = r["gr_no"]
            detail = bilty_detail_map.get(gr) or station_detail_map.get(gr)
            if detail:
                merged.append(detail)

        # 6. Apply client-side filters (branch, search, payment, city)
        if branch_id:
            merged = [r for r in merged if r.get("branch_id") == branch_id]
        if search:
            s_lower = search.lower()
            merged = [r for r in merged if (
                s_lower in (r.get("gr_no") or "").lower()
                or s_lower in (r.get("consignor_name") or r.get("consignor") or "").lower()
                or s_lower in (r.get("consignee_name") or r.get("consignee") or "").lower()
                or s_lower in (r.get("transport_name") or "").lower()
            )]
        if payment_mode:
            merged = [r for r in merged if
                      r.get("payment_mode") == payment_mode or r.get("payment_status") == payment_mode]
        if city_id:
            merged = [r for r in merged if
                      r.get("to_city_id") == city_id or r.get("city_id") == city_id]

        total = len(merged)
        regular_count = sum(1 for r in merged if r.get("source_table") == "bilty")
        station_count = total - regular_count

        # 7. Paginate
        offset = (page - 1) * page_size
        page_rows = merged[offset: offset + page_size]

        return {
            "status": "success",
            "data": {
                "rows": page_rows,
                "page": page,
                "page_size": page_size,
                "total": total,
                "has_more": (offset + page_size) < total,
                "regular_count": regular_count,
                "station_count": station_count,
            },
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── TRANSIT BILTIES (for a challan) ──────────────────────────

def get_transit_bilties(challan_no: str, page: int = 1,
                        page_size: int = PAGE_SIZE, search: str = None) -> dict:
    """Get all bilties assigned to a specific challan."""
    try:
        sb = get_supabase()
        q = sb.table("transit_details").select(TRANSIT_COLS, count="exact").eq("challan_no", challan_no)

        if search:
            q = q.or_(f"gr_no.ilike.%{search}%")

        offset = (page - 1) * page_size
        q = q.order("gr_no").range(offset, offset + page_size - 1)
        resp = q.execute()
        rows = resp.data or []
        total = resp.count if resp.count is not None else len(rows)

        # Enrich with bilty details
        bilty_ids = [r["bilty_id"] for r in rows if r.get("bilty_id")]
        gr_nos = [r["gr_no"] for r in rows]
        bilty_map = {}
        station_map = {}

        if bilty_ids:
            b_resp = sb.table("bilty").select(
                "id, gr_no, consignor_name, consignee_name, transport_name, "
                "payment_mode, no_of_pkg, wt, total, to_city_id, e_way_bill, pvt_marks"
            ).in_("id", bilty_ids).execute()
            bilty_map = {b["id"]: b for b in (b_resp.data or [])}

        # For station bilties (no bilty_id), fetch from station_bilty_summary
        station_grs = [r["gr_no"] for r in rows if not r.get("bilty_id")]
        if station_grs:
            s_resp = sb.table("station_bilty_summary").select(
                "gr_no, consignor, consignee, transport_name, "
                "payment_status, no_of_packets, weight, amount, city_id, e_way_bill, pvt_marks"
            ).in_("gr_no", station_grs).execute()
            station_map = {s["gr_no"]: s for s in (s_resp.data or [])}

        for r in rows:
            if r.get("bilty_id") and r["bilty_id"] in bilty_map:
                b = bilty_map[r["bilty_id"]]
                r["consignor_name"] = b.get("consignor_name")
                r["consignee_name"] = b.get("consignee_name")
                r["transport_name"] = b.get("transport_name")
                r["payment_mode"] = b.get("payment_mode")
                r["no_of_pkg"] = b.get("no_of_pkg")
                r["wt"] = b.get("wt")
                r["total"] = b.get("total")
                r["to_city_id"] = b.get("to_city_id")
                r["e_way_bill"] = b.get("e_way_bill")
                r["pvt_marks"] = b.get("pvt_marks")
                r["source_table"] = "bilty"
            elif r["gr_no"] in station_map:
                s = station_map[r["gr_no"]]
                r["consignor_name"] = s.get("consignor")
                r["consignee_name"] = s.get("consignee")
                r["transport_name"] = s.get("transport_name")
                r["payment_mode"] = s.get("payment_status")
                r["no_of_pkg"] = s.get("no_of_packets")
                r["wt"] = s.get("weight")
                r["total"] = s.get("amount")
                r["to_city_id"] = s.get("city_id")
                r["e_way_bill"] = s.get("e_way_bill")
                r["pvt_marks"] = s.get("pvt_marks")
                r["source_table"] = "station_bilty_summary"

        return {
            "status": "success",
            "data": {"rows": rows, "page": page, "page_size": page_size,
                     "total": total, "has_more": (offset + page_size) < total},
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── ADD BILTIES TO TRANSIT ────────────────────────────────────

def add_to_transit(data: dict) -> dict:
    """
    Add bilties to a challan's transit.
    data: {
        challan_id: str,        # challan_details.id
        challan_book_id: str,   # challan_books.id
        bilties: [              # list of bilties to add
            { gr_no: str, bilty_id: str|null, source_table: "bilty"|"station_bilty_summary" }
        ],
        user_id: str            # created_by
    }

    VALIDATIONS:
    - Challan must exist and NOT be dispatched
    - No duplicate GR numbers in transit
    - Deduplication: prefer 'bilty' over 'station_bilty_summary' for same gr_no
    """
    try:
        sb = get_supabase()

        challan_id = data.get("challan_id")
        book_id = data.get("challan_book_id")
        bilties = data.get("bilties", [])
        user_id = data.get("user_id")

        if not challan_id:
            return {"status": "error", "message": "challan_id is required", "status_code": 400}
        if not book_id:
            return {"status": "error", "message": "challan_book_id is required", "status_code": 400}
        if not bilties:
            return {"status": "error", "message": "bilties array is required and must not be empty", "status_code": 400}
        if not user_id:
            return {"status": "error", "message": "user_id is required", "status_code": 400}

        # 1. Validate challan — MUST NOT be dispatched
        challan_resp = sb.table("challan_details").select(
            "id, challan_no, branch_id, is_dispatched, is_active, total_bilty_count"
        ).eq("id", challan_id).single().execute()
        challan = challan_resp.data
        if not challan:
            return {"status": "error", "message": "Challan not found", "status_code": 404}
        if not challan.get("is_active"):
            return {"status": "error", "message": "Challan is deleted", "status_code": 400}
        if challan.get("is_dispatched"):
            return {"status": "error", "message": "Cannot add bilties to a dispatched challan", "status_code": 400}

        challan_no = challan["challan_no"]
        branch_id = challan["branch_id"]

        # 2. Get challan book for destination branch
        book_resp = sb.table("challan_books").select(
            "id, to_branch_id"
        ).eq("id", book_id).single().execute()
        book = book_resp.data
        if not book:
            return {"status": "error", "message": "Challan book not found", "status_code": 404}
        to_branch_id = book["to_branch_id"]

        # 3. Deduplicate input — prefer 'bilty' over 'station_bilty_summary'
        bilty_map = {}
        for b in bilties:
            gr = b.get("gr_no")
            if not gr:
                continue
            existing = bilty_map.get(gr)
            if not existing or b.get("source_table") == "bilty":
                bilty_map[gr] = b

        if not bilty_map:
            return {"status": "error", "message": "No valid bilties provided", "status_code": 400}

        # 4. Check which GR numbers already exist in ANY transit
        gr_list = list(bilty_map.keys())
        existing_resp = sb.table("transit_details").select("gr_no").in_("gr_no", gr_list).execute()
        already_in_transit = {r["gr_no"] for r in (existing_resp.data or [])}

        # 5. Build insert rows (skip duplicates)
        now = _now()
        to_insert = []
        skipped = []
        for gr, b in bilty_map.items():
            if gr in already_in_transit:
                skipped.append(gr)
                continue
            row = {
                "challan_no": challan_no,
                "gr_no": gr,
                "bilty_id": b.get("bilty_id") if b.get("source_table") == "bilty" else None,
                "challan_book_id": book_id,
                "from_branch_id": branch_id,
                "to_branch_id": to_branch_id,
                "is_out_of_delivery_from_branch1": False,
                "is_delivered_at_branch2": False,
                "is_out_of_delivery_from_branch2": False,
                "is_delivered_at_destination": False,
                "out_for_door_delivery": False,
                "created_by": user_id,
                "created_at": now,
                "updated_at": now,
            }
            to_insert.append(row)

        if not to_insert:
            return {
                "status": "success",
                "message": f"No new bilties to add. {len(skipped)} already in transit.",
                "data": {"added": 0, "skipped": skipped},
            }

        # 6. Insert transit rows
        resp = sb.table("transit_details").insert(to_insert).execute()
        added_count = len(resp.data) if resp.data else 0

        # 7. Update challan bilty count
        current_count = challan.get("total_bilty_count") or 0
        sb.table("challan_details").update({
            "total_bilty_count": current_count + added_count,
            "updated_at": now,
        }).eq("id", challan_id).execute()

        # Re-fetch updated count
        updated_challan = sb.table("challan_details").select("total_bilty_count").eq("id", challan_id).single().execute()

        return {
            "status": "success",
            "message": f"{added_count} bilties added to challan {challan_no}",
            "data": {
                "added": added_count,
                "skipped": skipped,
                "total_bilty_count": updated_challan.data.get("total_bilty_count") if updated_challan.data else added_count,
            },
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── REMOVE SINGLE BILTY FROM TRANSIT ─────────────────────────

def remove_from_transit(transit_id: str, challan_id: str = None) -> dict:
    """Remove a single bilty from transit by transit_details.id."""
    try:
        sb = get_supabase()

        # Get transit record
        t_resp = sb.table("transit_details").select("id, challan_no").eq("id", transit_id).single().execute()
        if not t_resp.data:
            return {"status": "error", "message": "Transit record not found", "status_code": 404}

        challan_no = t_resp.data["challan_no"]

        # Validate challan is not dispatched
        c_resp = sb.table("challan_details").select(
            "id, is_dispatched, total_bilty_count"
        ).eq("challan_no", challan_no).eq("is_active", True).single().execute()

        if c_resp.data and c_resp.data.get("is_dispatched"):
            return {"status": "error", "message": "Cannot remove bilty from a dispatched challan", "status_code": 400}

        # Delete transit row
        sb.table("transit_details").delete().eq("id", transit_id).execute()

        # Update challan count
        if c_resp.data:
            new_count = max(0, (c_resp.data.get("total_bilty_count") or 1) - 1)
            sb.table("challan_details").update({
                "total_bilty_count": new_count,
                "updated_at": _now(),
            }).eq("id", c_resp.data["id"]).execute()

        return {"status": "success", "message": "Bilty removed from transit",
                "data": {"total_bilty_count": new_count if c_resp.data else None}}
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── BULK REMOVE FROM TRANSIT ─────────────────────────────────

def bulk_remove_from_transit(transit_ids: list, challan_id: str = None) -> dict:
    """Remove multiple bilties from transit."""
    if not transit_ids:
        return {"status": "error", "message": "transit_ids array is required", "status_code": 400}

    try:
        sb = get_supabase()

        # Get challan_no from first transit record to validate dispatch status
        first_resp = sb.table("transit_details").select("challan_no").in_("id", transit_ids).limit(1).execute()
        if not first_resp.data:
            return {"status": "error", "message": "No transit records found", "status_code": 404}

        challan_no = first_resp.data[0]["challan_no"]

        # Validate challan is not dispatched
        c_resp = sb.table("challan_details").select(
            "id, is_dispatched, total_bilty_count"
        ).eq("challan_no", challan_no).eq("is_active", True).single().execute()

        if c_resp.data and c_resp.data.get("is_dispatched"):
            return {"status": "error", "message": "Cannot remove bilties from a dispatched challan", "status_code": 400}

        # Delete all transit rows
        del_resp = sb.table("transit_details").delete().in_("id", transit_ids).execute()
        removed = len(del_resp.data) if del_resp.data else 0

        # Update challan count
        if c_resp.data:
            new_count = max(0, (c_resp.data.get("total_bilty_count") or 0) - removed)
            sb.table("challan_details").update({
                "total_bilty_count": new_count,
                "updated_at": _now(),
            }).eq("id", c_resp.data["id"]).execute()

        return {
            "status": "success",
            "message": f"{removed} bilties removed from transit",
            "data": {"removed": removed, "total_bilty_count": new_count if c_resp.data else None},
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── BULK UPDATE DELIVERY STATUS ───────────────────────────────

def bulk_update_delivery_status(updates: list, user_id: str = None) -> dict:
    """
    Update delivery pipeline stages for multiple transit records.
    updates = [
        {
            "id": "transit-uuid",
            "stage": "out_from_branch1" | "delivered_at_branch2" |
                     "out_from_branch2" | "delivered_at_destination" | "door_delivery",
            ... extra fields like delivery_agent_name, vehicle_number
        }
    ]
    """
    if not updates:
        return {"status": "error", "message": "updates array is required", "status_code": 400}

    STAGE_MAP = {
        "out_from_branch1": ("is_out_of_delivery_from_branch1", "out_of_delivery_from_branch1_date"),
        "delivered_at_branch2": ("is_delivered_at_branch2", "delivered_at_branch2_date"),
        "out_from_branch2": ("is_out_of_delivery_from_branch2", "out_of_delivery_from_branch2_date"),
        "delivered_at_destination": ("is_delivered_at_destination", "delivered_at_destination_date"),
        "door_delivery": ("out_for_door_delivery", "out_for_door_delivery_date"),
    }

    try:
        sb = get_supabase()
        now = _now()
        success = 0
        failed = []

        for item in updates:
            tid = item.get("id")
            stage = item.get("stage")
            if not tid or not stage:
                failed.append({"id": tid, "error": "Missing id or stage"})
                continue
            if stage not in STAGE_MAP:
                failed.append({"id": tid, "error": f"Invalid stage: {stage}. Valid: {list(STAGE_MAP.keys())}"})
                continue

            flag_col, date_col = STAGE_MAP[stage]
            payload = {
                flag_col: True,
                date_col: now,
                "updated_at": now,
            }
            if user_id:
                payload["updated_by"] = user_id

            # Extra fields for door delivery
            if stage == "door_delivery":
                if item.get("delivery_agent_name"):
                    payload["delivery_agent_name"] = item["delivery_agent_name"]
                if item.get("delivery_agent_phone"):
                    payload["delivery_agent_phone"] = item["delivery_agent_phone"]
                if item.get("vehicle_number"):
                    payload["vehicle_number"] = item["vehicle_number"]

            if item.get("remarks"):
                payload["remarks"] = item["remarks"]

            try:
                sb.table("transit_details").update(payload).eq("id", tid).execute()
                success += 1
            except Exception as e:
                failed.append({"id": tid, "error": str(e)})

        return {
            "status": "success",
            "message": f"Delivery status updated: {success} success, {len(failed)} failed",
            "data": {"success_count": success, "failed_count": len(failed), "failed": failed},
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}


# ── CHALLAN STATS ─────────────────────────────────────────────

def get_challan_stats(challan_no: str) -> dict:
    """Get stats for a challan: counts, weight, ewb count, payment breakdown."""
    try:
        sb = get_supabase()
        t_resp = sb.table("transit_details").select("gr_no, bilty_id").eq("challan_no", challan_no).execute()
        rows = t_resp.data or []

        if not rows:
            return {"status": "success", "data": {
                "total": 0, "regular": 0, "station": 0,
                "total_weight": 0, "total_packages": 0, "ewb_count": 0,
                "topay_amount": 0, "paid_amount": 0,
            }}

        bilty_ids = [r["bilty_id"] for r in rows if r.get("bilty_id")]
        station_grs = [r["gr_no"] for r in rows if not r.get("bilty_id")]

        total_weight = 0
        total_packages = 0
        ewb_count = 0
        topay_amount = 0
        paid_amount = 0

        if bilty_ids:
            b_resp = sb.table("bilty").select(
                "id, wt, no_of_pkg, e_way_bill, payment_mode, total"
            ).in_("id", bilty_ids).execute()
            for b in (b_resp.data or []):
                total_weight += float(b.get("wt") or 0)
                total_packages += int(b.get("no_of_pkg") or 0)
                if b.get("e_way_bill"):
                    ewb_count += 1
                if b.get("payment_mode", "").upper() in ("TO-PAY", "TO-PAY/DD"):
                    topay_amount += float(b.get("total") or 0)
                else:
                    paid_amount += float(b.get("total") or 0)

        if station_grs:
            s_resp = sb.table("station_bilty_summary").select(
                "gr_no, weight, no_of_packets, e_way_bill, payment_status, amount"
            ).in_("gr_no", station_grs).execute()
            for s in (s_resp.data or []):
                total_weight += float(s.get("weight") or 0)
                total_packages += int(s.get("no_of_packets") or 0)
                if s.get("e_way_bill"):
                    ewb_count += 1
                if s.get("payment_status", "").upper() in ("TO-PAY", "TO-PAY/DD"):
                    topay_amount += float(s.get("amount") or 0)
                else:
                    paid_amount += float(s.get("amount") or 0)

        return {
            "status": "success",
            "data": {
                "total": len(rows),
                "regular": len(bilty_ids),
                "station": len(station_grs),
                "total_weight": round(total_weight, 2),
                "total_packages": total_packages,
                "ewb_count": ewb_count,
                "topay_amount": round(topay_amount, 2),
                "paid_amount": round(paid_amount, 2),
            },
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "status_code": 500}
