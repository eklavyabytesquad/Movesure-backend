"""
GR Reservation Service
Atomic GR number reservation, release, completion, and sequence management.
All operations use row-level checks to prevent race conditions.
"""
from datetime import datetime, timedelta, timezone
from concurrent.futures import as_completed
from services.supabase_client import get_supabase
from services.thread_pool import shared_pool

RESERVATION_TTL_MINUTES = 30


def _find_highest_used_number(used_grs: set, prefix: str, postfix: str) -> int | None:
    """
    Parse GR strings and return the highest numeric part.
    Returns None if used_grs is empty.
    """
    if not used_grs:
        return None
    prefix_len = len(prefix or "")
    postfix_len = len(postfix or "")
    max_num = None
    for gr in used_grs:
        try:
            num_part = gr[prefix_len:] if not postfix_len else gr[prefix_len:-postfix_len]
            num = int(num_part)
            if max_num is None or num > max_num:
                max_num = num
        except (ValueError, IndexError):
            continue
    return max_num


def _format_gr(prefix: str, number: int, digits: int, postfix: str = None) -> str:
    """Format a GR number string, e.g. prefix='A', number=8044, digits=5 -> 'A08044'."""
    gr = f"{prefix or ''}{str(number).zfill(digits)}{postfix or ''}"
    return gr


def _get_bill_book(sb, bill_book_id: str) -> dict | None:
    """Fetch a single bill book by ID."""
    resp = sb.table("bill_books").select("*").eq("id", bill_book_id).single().execute()
    return resp.data


def _get_active_reservations(sb, branch_id: str, bill_book_id: str = None) -> list:
    """Get all active (non-expired) reservations for a branch."""
    now = datetime.now(timezone.utc).isoformat()
    q = (
        sb.table("gr_reservations")
        .select("*")
        .eq("branch_id", branch_id)
        .eq("status", "reserved")
        .gte("expires_at", now)
    )
    if bill_book_id:
        q = q.eq("bill_book_id", bill_book_id)
    return q.order("gr_number").execute().data or []


def _get_used_gr_numbers(sb, bill_book_id: str, prefix: str, digits: int, from_num: int, to_num: int) -> set:
    """Get all GR numbers already saved as bilties for this bill book range."""
    # Build the set of gr_no strings that exist in the bilty table
    # We query bilty where gr_no starts with prefix and is in the range
    gr_start = _format_gr(prefix, from_num, digits)
    gr_end = _format_gr(prefix, to_num, digits)
    resp = (
        sb.table("bilty")
        .select("gr_no")
        .gte("gr_no", gr_start)
        .lte("gr_no", gr_end)
        .eq("is_active", True)
        .execute()
    )
    return {r["gr_no"] for r in (resp.data or [])}


def _find_next_available(
    current_number: int,
    to_number: int,
    prefix: str,
    digits: int,
    postfix: str,
    reserved_grs: set,
    used_grs: set,
    count: int = 1,
) -> list[dict]:
    """
    Starting from current_number, find `count` GR numbers that are
    neither reserved nor used. Returns list of {number, gr_no}.
    """
    results = []
    n = current_number
    while n <= to_number and len(results) < count:
        gr = _format_gr(prefix, n, digits, postfix)
        if gr not in reserved_grs and gr not in used_grs:
            results.append({"number": n, "gr_no": gr})
        n += 1
    return results


# ────────────────────────────────────────────────────
# PUBLIC API FUNCTIONS
# ────────────────────────────────────────────────────


def get_next_available_grs(bill_book_id: str, branch_id: str, count: int = 5) -> dict:
    """
    Return the next `count` available GR numbers for a bill book.
    Skips reserved + already-used numbers.
    """
    try:
        sb = get_supabase()
        bb = _get_bill_book(sb, bill_book_id)
        if not bb:
            return {"status": "error", "message": "Bill book not found", "status_code": 404}

        # Parallel fetch: reservations + used bilties
        f_res = shared_pool.submit(_get_active_reservations, sb, branch_id, bill_book_id)
        f_used = shared_pool.submit(
            _get_used_gr_numbers, sb, bill_book_id,
            bb["prefix"], bb["digits"], bb["from_number"], bb["to_number"],
        )
        reservations = f_res.result()
        used_grs = f_used.result()

        reserved_grs = {r["gr_no"] for r in reservations}

        available = _find_next_available(
            bb["current_number"], bb["to_number"],
            bb["prefix"], bb["digits"], bb.get("postfix"),
            reserved_grs, used_grs, count,
        )

        return {
            "status": "success",
            "data": {
                "bill_book_id": bill_book_id,
                "available": available,
                "current_number": bb["current_number"],
                "reserved_count": len(reservations),
            },
        }
    except Exception as e:
        return {"status": "error", "message": f"Failed to get available GRs: {e}", "status_code": 500}


def reserve_gr(bill_book_id: str, branch_id: str, user_id: str, user_name: str, gr_number: int = None) -> dict:
    """
    Reserve a GR number. If gr_number is provided, reserve that specific one.
    Otherwise, reserve the next available number.
    Also auto-fixes current_number if it's behind the last used bilty.
    """
    try:
        sb = get_supabase()
        bb = _get_bill_book(sb, bill_book_id)
        if not bb:
            return {"status": "error", "message": "Bill book not found", "status_code": 404}

        prefix = bb["prefix"]
        digits = bb["digits"]
        postfix = bb.get("postfix")
        current = bb["current_number"]
        to_num = bb["to_number"]

        # Get existing reservations + used bilties
        f_res = shared_pool.submit(_get_active_reservations, sb, branch_id, bill_book_id)
        f_used = shared_pool.submit(
            _get_used_gr_numbers, sb, bill_book_id,
            prefix, digits, bb["from_number"], to_num,
        )
        reservations = f_res.result()
        used_grs = f_used.result()

        reserved_grs = {r["gr_no"] for r in reservations}

        # ── Auto-fix current_number if it points to an already-used GR ──
        test_gr = _format_gr(prefix, current, digits, postfix)
        if test_gr in used_grs:
            # Find the correct current_number (first unused number)
            fixed = current
            while fixed <= to_num and _format_gr(prefix, fixed, digits, postfix) in used_grs:
                fixed += 1
            if fixed != current and fixed <= to_num:
                sb.table("bill_books").update({"current_number": fixed}).eq("id", bill_book_id).execute()
                current = fixed
                print(f"🔧 Auto-fixed current_number from {bb['current_number']} to {fixed}")

        if gr_number is not None:
            # Reserve a specific number
            if gr_number < bb["from_number"] or gr_number > to_num:
                return {"status": "error", "message": f"GR number {gr_number} is out of bill book range ({bb['from_number']}-{to_num})", "status_code": 400}
            target_gr = _format_gr(prefix, gr_number, digits, postfix)
            if target_gr in reserved_grs:
                # Find who reserved it
                owner = next((r for r in reservations if r["gr_no"] == target_gr), None)
                return {"status": "error", "message": f"GR {target_gr} is already reserved by {owner.get('user_name', 'another user') if owner else 'another user'}", "status_code": 409}
            if target_gr in used_grs:
                return {"status": "error", "message": f"GR {target_gr} is already used in a saved bilty", "status_code": 409}
            chosen_number = gr_number
            chosen_gr = target_gr
        else:
            # Reserve next available
            available = _find_next_available(
                current, to_num, prefix, digits, postfix,
                reserved_grs, used_grs, 1,
            )
            if not available:
                return {"status": "error", "message": "No available GR numbers in this bill book", "status_code": 409}
            chosen_number = available[0]["number"]
            chosen_gr = available[0]["gr_no"]

        # Check for existing active reservation by this user for this bill book
        existing_user_res = [
            r for r in reservations
            if r["user_id"] == user_id
        ]
        # Allow multiple reservations per user (batch mode)

        now = datetime.now(timezone.utc)
        expires = now + timedelta(minutes=RESERVATION_TTL_MINUTES)

        row = {
            "bill_book_id": bill_book_id,
            "branch_id": branch_id,
            "gr_no": chosen_gr,
            "gr_number": chosen_number,
            "user_id": user_id,
            "user_name": user_name,
            "status": "reserved",
            "reserved_at": now.isoformat(),
            "expires_at": expires.isoformat(),
        }

        inserted = sb.table("gr_reservations").insert(row).execute()
        if not inserted.data:
            return {"status": "error", "message": "Failed to create reservation", "status_code": 500}

        reservation = inserted.data[0]

        return {
            "status": "success",
            "data": {
                "reservation": reservation,
                "gr_no": chosen_gr,
                "gr_number": chosen_number,
                "expires_at": expires.isoformat(),
                "auto_fixed_current_number": current != bb["current_number"],
            },
        }
    except Exception as e:
        if "duplicate" in str(e).lower() or "unique" in str(e).lower():
            return {"status": "error", "message": f"GR number already reserved (race condition). Please retry.", "status_code": 409}
        return {"status": "error", "message": f"Failed to reserve GR: {e}", "status_code": 500}


def release_reservation(reservation_id: str, user_id: str) -> dict:
    """Release a reservation (user cancelled / no longer needs it)."""
    try:
        sb = get_supabase()
        # Verify ownership
        res = sb.table("gr_reservations").select("*").eq("id", reservation_id).single().execute()
        if not res.data:
            return {"status": "error", "message": "Reservation not found", "status_code": 404}

        reservation = res.data
        if reservation["user_id"] != user_id:
            return {"status": "error", "message": "You can only release your own reservations", "status_code": 403}
        if reservation["status"] != "reserved":
            return {"status": "error", "message": f"Reservation is already {reservation['status']}", "status_code": 400}

        now = datetime.now(timezone.utc).isoformat()
        sb.table("gr_reservations").update({
            "status": "released",
            "released_at": now,
        }).eq("id", reservation_id).execute()

        return {
            "status": "success",
            "data": {"reservation_id": reservation_id, "gr_no": reservation["gr_no"], "released_at": now},
        }
    except Exception as e:
        return {"status": "error", "message": f"Failed to release: {e}", "status_code": 500}


def complete_reservation(reservation_id: str, user_id: str) -> dict:
    """
    Complete a reservation after bilty save.
    Marks reservation as 'used' and sets bill_books.current_number
    to highest_used_bilty + 1 (safety-checked, prevents drift).
    """
    try:
        sb = get_supabase()
        res = sb.table("gr_reservations").select("*").eq("id", reservation_id).single().execute()
        if not res.data:
            return {"status": "error", "message": "Reservation not found", "status_code": 404}

        reservation = res.data
        if reservation["user_id"] != user_id:
            return {"status": "error", "message": "You can only complete your own reservations", "status_code": 403}
        if reservation["status"] != "reserved":
            return {"status": "error", "message": f"Reservation is already {reservation['status']}", "status_code": 400}

        now = datetime.now(timezone.utc).isoformat()
        gr_number = reservation["gr_number"]
        bill_book_id = reservation["bill_book_id"]

        # 1. Mark reservation as used
        sb.table("gr_reservations").update({
            "status": "used",
            "used_at": now,
        }).eq("id", reservation_id).execute()

        # 2. Safety check: set current_number = highest_used_bilty + 1
        #    This is idempotent with save_bilty's safety check — both always
        #    compute the same value, so no double-increment can occur.
        new_current = None
        bb = _get_bill_book(sb, bill_book_id)
        if bb:
            used_grs = _get_used_gr_numbers(
                sb, bill_book_id, bb["prefix"], bb["digits"],
                bb["from_number"], bb["to_number"],
            )
            highest = _find_highest_used_number(used_grs, bb["prefix"], bb.get("postfix"))
            if highest is not None:
                new_current = highest + 1
            else:
                new_current = bb["from_number"]
            # Wraparound / clamp
            if new_current > bb["to_number"]:
                if bb.get("auto_continue"):
                    new_current = bb["from_number"]
                else:
                    new_current = bb["to_number"]
            if new_current != bb["current_number"]:
                sb.table("bill_books").update({
                    "current_number": new_current,
                }).eq("id", bill_book_id).execute()

        return {
            "status": "success",
            "data": {
                "reservation_id": reservation_id,
                "gr_no": reservation["gr_no"],
                "used_at": now,
                "new_current_number": new_current,
            },
        }
    except Exception as e:
        return {"status": "error", "message": f"Failed to complete reservation: {e}", "status_code": 500}


def extend_reservation(reservation_id: str, user_id: str) -> dict:
    """Heartbeat — extend the TTL by another 30 minutes."""
    try:
        sb = get_supabase()
        res = sb.table("gr_reservations").select("id, user_id, status").eq("id", reservation_id).single().execute()
        if not res.data:
            return {"status": "error", "message": "Reservation not found", "status_code": 404}
        if res.data["user_id"] != user_id:
            return {"status": "error", "message": "Not your reservation", "status_code": 403}
        if res.data["status"] != "reserved":
            return {"status": "error", "message": f"Reservation is {res.data['status']}", "status_code": 400}

        new_expiry = (datetime.now(timezone.utc) + timedelta(minutes=RESERVATION_TTL_MINUTES)).isoformat()
        sb.table("gr_reservations").update({"expires_at": new_expiry}).eq("id", reservation_id).execute()

        return {"status": "success", "data": {"reservation_id": reservation_id, "expires_at": new_expiry}}
    except Exception as e:
        return {"status": "error", "message": f"Failed to extend: {e}", "status_code": 500}


def get_branch_gr_status(branch_id: str, bill_book_id: str = None) -> dict:
    """
    Get full GR status for a branch: active reservations, recent bilties, bill book state.
    This is the main 'live status' endpoint the UI subscribes to.
    """
    try:
        sb = get_supabase()

        def fetch_reservations():
            return _get_active_reservations(sb, branch_id, bill_book_id)

        def fetch_recent_bilties():
            """Last 20 bilties created in this branch (last 24h)."""
            since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
            q = (
                sb.table("bilty")
                .select("id, gr_no, consignor_name, created_at")
                .eq("branch_id", branch_id)
                .eq("is_active", True)
                .gte("created_at", since)
                .order("created_at", desc=True)
                .limit(20)
            )
            return q.execute().data or []

        def fetch_bill_book():
            if not bill_book_id:
                return None
            return _get_bill_book(sb, bill_book_id)

        f1 = shared_pool.submit(fetch_reservations)
        f2 = shared_pool.submit(fetch_recent_bilties)
        f3 = shared_pool.submit(fetch_bill_book)
        reservations = f1.result()
        recent = f2.result()
        bb = f3.result()

        return {
            "status": "success",
            "data": {
                "reservations": reservations,
                "recent_bilties": recent,
                "bill_book": bb,
                "reservation_count": len(reservations),
            },
        }
    except Exception as e:
        return {"status": "error", "message": f"Failed to get branch status: {e}", "status_code": 500}


def release_all_user_reservations(user_id: str, branch_id: str) -> dict:
    """Release ALL active reservations for a user in a branch (logout / page close)."""
    try:
        sb = get_supabase()
        now_str = datetime.now(timezone.utc).isoformat()

        # Fetch user's active reservations
        active = (
            sb.table("gr_reservations")
            .select("id, gr_no")
            .eq("user_id", user_id)
            .eq("branch_id", branch_id)
            .eq("status", "reserved")
            .gte("expires_at", now_str)
            .execute()
        ).data or []

        if not active:
            return {"status": "success", "data": {"released_count": 0}}

        ids = [r["id"] for r in active]
        for rid in ids:
            sb.table("gr_reservations").update({
                "status": "released",
                "released_at": now_str,
            }).eq("id", rid).execute()

        return {
            "status": "success",
            "data": {
                "released_count": len(ids),
                "released_grs": [r["gr_no"] for r in active],
            },
        }
    except Exception as e:
        return {"status": "error", "message": f"Failed to release all: {e}", "status_code": 500}


def fix_gr_sequence(bill_book_id: str, correct_number: int = None) -> dict:
    """
    Fix the bill book's current_number.
    If correct_number is provided, set it directly (must be in range).
    Otherwise auto-detect: find the highest used GR and set current_number = that + 1.
    """
    try:
        sb = get_supabase()
        bb = _get_bill_book(sb, bill_book_id)
        if not bb:
            return {"status": "error", "message": "Bill book not found", "status_code": 404}

        if correct_number is not None:
            if correct_number < bb["from_number"] or correct_number > bb["to_number"]:
                return {
                    "status": "error",
                    "message": f"Number {correct_number} is out of range ({bb['from_number']}-{bb['to_number']})",
                    "status_code": 400,
                }
            new_current = correct_number
        else:
            # Auto-detect: find highest used GR number
            used_grs = _get_used_gr_numbers(
                sb, bill_book_id, bb["prefix"], bb["digits"],
                bb["from_number"], bb["to_number"],
            )
            highest = _find_highest_used_number(used_grs, bb["prefix"], bb.get("postfix"))
            if highest is not None:
                new_current = highest + 1
            else:
                new_current = bb["from_number"]

            # Also check reservations — don't set current_number below any active reservation
            reservations = _get_active_reservations(sb, bb.get("branch_id", ""), bill_book_id)
            if reservations:
                max_reserved = max(r["gr_number"] for r in reservations)
                new_current = max(new_current, max_reserved + 1)

        old = bb["current_number"]
        if new_current > bb["to_number"]:
            new_current = bb["to_number"]

        sb.table("bill_books").update({"current_number": new_current}).eq("id", bill_book_id).execute()

        return {
            "status": "success",
            "data": {
                "bill_book_id": bill_book_id,
                "old_current_number": old,
                "new_current_number": new_current,
                "fixed": old != new_current,
            },
        }
    except Exception as e:
        return {"status": "error", "message": f"Failed to fix sequence: {e}", "status_code": 500}


def cleanup_expired_reservations(branch_id: str = None) -> dict:
    """Mark expired reservations as 'expired'. Run periodically or on-demand."""
    try:
        sb = get_supabase()
        now_str = datetime.now(timezone.utc).isoformat()

        q = (
            sb.table("gr_reservations")
            .select("id")
            .eq("status", "reserved")
            .lt("expires_at", now_str)
        )
        if branch_id:
            q = q.eq("branch_id", branch_id)

        expired = q.execute().data or []
        for r in expired:
            sb.table("gr_reservations").update({
                "status": "expired",
                "released_at": now_str,
            }).eq("id", r["id"]).execute()

        return {
            "status": "success",
            "data": {"expired_count": len(expired)},
        }
    except Exception as e:
        return {"status": "error", "message": f"Cleanup failed: {e}", "status_code": 500}


def validate_bill_book(bill_book_id: str) -> dict:
    """
    Validate and auto-correct a bill book's current_number.
    Called every time the UI loads/edits a bill book.
    Checks:
      1. Is current_number pointing to a GR that already exists as a bilty?
      2. If yes, advance current_number past ALL used GR numbers.
      3. Also skip any actively reserved numbers.
    Returns the corrected bill book state + list of corrections made.
    """
    try:
        sb = get_supabase()
        bb = _get_bill_book(sb, bill_book_id)
        if not bb:
            return {"status": "error", "message": "Bill book not found", "status_code": 404}

        prefix = bb["prefix"]
        digits = bb["digits"]
        postfix = bb.get("postfix")
        from_num = bb["from_number"]
        to_num = bb["to_number"]
        old_current = bb["current_number"]
        branch_id = bb.get("branch_id", "")

        # Parallel: get used GRs + active reservations
        f_used = shared_pool.submit(_get_used_gr_numbers, sb, bill_book_id, prefix, digits, from_num, to_num)
        f_res = shared_pool.submit(_get_active_reservations, sb, branch_id, bill_book_id)
        used_grs = f_used.result()
        reservations = f_res.result()

        reserved_grs = {r["gr_no"] for r in reservations}

        # Check if current_number's GR is already used
        current_gr = _format_gr(prefix, old_current, digits, postfix)
        needs_fix = current_gr in used_grs

        # Find the correct next available number
        new_current = old_current
        while new_current <= to_num:
            gr = _format_gr(prefix, new_current, digits, postfix)
            if gr not in used_grs and gr not in reserved_grs:
                break
            new_current += 1

        if new_current > to_num:
            # Bill book exhausted
            return {
                "status": "success",
                "data": {
                    "bill_book_id": bill_book_id,
                    "old_current_number": old_current,
                    "new_current_number": new_current,
                    "fixed": False,
                    "exhausted": True,
                    "message": "Bill book is full — no available numbers left",
                    "bill_book": bb,
                },
            }

        fixed = new_current != old_current
        if fixed:
            sb.table("bill_books").update({"current_number": new_current}).eq("id", bill_book_id).execute()
            bb["current_number"] = new_current

        # Build next 5 available for convenience
        available = _find_next_available(
            new_current, to_num, prefix, digits, postfix,
            reserved_grs, used_grs, 5,
        )

        return {
            "status": "success",
            "data": {
                "bill_book_id": bill_book_id,
                "old_current_number": old_current,
                "new_current_number": new_current,
                "fixed": fixed,
                "exhausted": False,
                "next_available": available,
                "reserved_count": len(reservations),
                "used_count": len(used_grs),
                "bill_book": bb,
            },
        }
    except Exception as e:
        return {"status": "error", "message": f"Failed to validate bill book: {e}", "status_code": 500}
