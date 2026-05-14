"""
Payment Tracking Service for Bilty
Handles payment details: mode, advance, remaining, payment status
Supports both individual and bulk payment updates
"""

from datetime import datetime, timezone
from typing import Optional, Dict, List
from services.supabase_client import get_supabase
import json


def _build_payment_details(
    payment_mode: str,
    advance_amount: float = 0,
    remaining_amount: float = 0,
    paid_amount: float = 0,
    payment_date: Optional[str] = None,
    payment_method: Optional[str] = None,
    reference_number: Optional[str] = None,
    notes: Optional[str] = None,
    transactions: Optional[List[Dict]] = None
) -> Dict:
    """Build payment_details JSONB object."""
    return {
        "payment_mode": payment_mode,
        "advance_amount": float(advance_amount) if advance_amount else 0,
        "remaining_amount": float(remaining_amount) if remaining_amount else 0,
        "paid_amount": float(paid_amount) if paid_amount else 0,
        "payment_date": payment_date,
        "payment_method": payment_method,
        "reference_number": reference_number,
        "notes": notes,
        "transactions": transactions or [],
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat()
    }


def _get_payment_status(payment_mode: str, advance_amount: float, total_amount: float) -> str:
    """Determine payment status based on mode and amounts."""
    if payment_mode == "foc":
        return "FOC"

    advance = float(advance_amount) if advance_amount else 0
    total = float(total_amount) if total_amount else 0

    if total <= 0:
        return "FOC"

    if advance >= total:
        return "PAID"
    elif advance > 0:
        return "PARTIAL"
    else:
        return "PENDING"


def _calculate_remaining(total_amount: float, advance_amount: float) -> float:
    """Calculate remaining amount."""
    total = float(total_amount) if total_amount else 0
    advance = float(advance_amount) if advance_amount else 0
    remaining = max(0, total - advance)
    return remaining


def save_bilty_payment(bilty_id: str, payment_data: dict) -> dict:
    """
    Save payment details for a bilty (non-draft).

    Args:
        bilty_id: UUID of the bilty
        payment_data: {
            "payment_mode": "cash|online|partial|foc",
            "advance_amount": <numeric>,
            "payment_date": "2026-05-15" (optional),
            "payment_method": "cash|cheque|bank_transfer|upi" (optional),
            "reference_number": "CHQ123456" (optional),
            "notes": "payment notes" (optional),
            "add_transaction": {...} (optional - add to transactions array)
        }

    Returns:
        {"status": "success", "bilty_id": "...", "payment_details": {...}}
        or error response
    """
    try:
        sb = get_supabase()

        # Fetch current bilty
        bilty_resp = sb.table("bilty").select(
            "id, total, payment_details, advance_amount"
        ).eq("id", bilty_id).single().execute()

        if not bilty_resp.data:
            return {
                "status": "error",
                "message": f"Bilty {bilty_id} not found",
                "status_code": 404
            }

        current_bilty = bilty_resp.data
        total_amount = float(current_bilty.get("total") or 0)

        # Extract payment data
        payment_mode = payment_data.get("payment_mode", "pending").lower()
        advance_amount = float(payment_data.get("advance_amount", 0))
        payment_date = payment_data.get("payment_date") or str(datetime.now().date())
        payment_method = payment_data.get("payment_method")
        reference_number = payment_data.get("reference_number")
        notes = payment_data.get("notes")

        # Validate
        if not payment_mode:
            return {
                "status": "error",
                "message": "payment_mode is required",
                "status_code": 400
            }

        if advance_amount < 0:
            return {
                "status": "error",
                "message": "advance_amount cannot be negative",
                "status_code": 400
            }

        if advance_amount > total_amount:
            return {
                "status": "error",
                "message": f"advance_amount ({advance_amount}) cannot exceed total ({total_amount})",
                "status_code": 400
            }

        # Build payment details
        remaining_amount = _calculate_remaining(total_amount, advance_amount)
        payment_status = _get_payment_status(payment_mode, advance_amount, total_amount)

        # Get existing transactions array
        existing_details = current_bilty.get("payment_details") or {}
        if isinstance(existing_details, str):
            existing_details = json.loads(existing_details)
        existing_transactions = existing_details.get("transactions", [])

        # Add new transaction if provided
        add_transaction = payment_data.get("add_transaction")
        if add_transaction:
            new_txn = {
                "date": add_transaction.get("date") or str(datetime.now().date()),
                "amount": float(add_transaction.get("amount")),
                "method": add_transaction.get("method") or payment_method,
                "reference": add_transaction.get("reference") or reference_number,
                "notes": add_transaction.get("notes")
            }
            existing_transactions.append(new_txn)

        # Build final payment_details
        payment_details = _build_payment_details(
            payment_mode=payment_mode,
            advance_amount=advance_amount,
            remaining_amount=remaining_amount,
            paid_amount=advance_amount,
            payment_date=payment_date,
            payment_method=payment_method,
            reference_number=reference_number,
            notes=notes,
            transactions=existing_transactions
        )

        # Update bilty
        update_data = {
            "payment_details": payment_details,
            "payment_status": payment_status,
            "advance_amount": advance_amount,
            "remaining_amount": remaining_amount,
            "payment_mode": payment_mode
        }

        update_resp = sb.table("bilty").update(update_data).eq(
            "id", bilty_id
        ).execute()

        if not update_resp.data:
            return {
                "status": "error",
                "message": "Failed to update payment details",
                "status_code": 500
            }

        return {
            "status": "success",
            "bilty_id": bilty_id,
            "payment_details": payment_details,
            "payment_status": payment_status,
            "advance_amount": advance_amount,
            "remaining_amount": remaining_amount
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "status_code": 500
        }


def save_station_bilty_payment(gr_no: str, payment_data: dict) -> dict:
    """
    Save payment details for a station_bilty_summary.

    Args:
        gr_no: GR number to identify the bilty
        payment_data: Same structure as save_bilty_payment

    Returns:
        {"status": "success", "gr_no": "...", "payment_details": {...}}
        or error response
    """
    try:
        sb = get_supabase()

        # Fetch current record
        bilty_resp = sb.table("station_bilty_summary").select(
            "id, gr_no, amount, payment_details, advance_amount"
        ).eq("gr_no", gr_no).single().execute()

        if not bilty_resp.data:
            return {
                "status": "error",
                "message": f"Station bilty with GR {gr_no} not found",
                "status_code": 404
            }

        current_record = bilty_resp.data
        total_amount = float(current_record.get("amount") or 0)

        # Extract payment data
        payment_mode = payment_data.get("payment_mode", "pending").lower()
        advance_amount = float(payment_data.get("advance_amount", 0))
        payment_date = payment_data.get("payment_date") or str(datetime.now().date())
        payment_method = payment_data.get("payment_method")
        reference_number = payment_data.get("reference_number")
        notes = payment_data.get("notes")

        # Validate
        if not payment_mode:
            return {
                "status": "error",
                "message": "payment_mode is required",
                "status_code": 400
            }

        if advance_amount < 0:
            return {
                "status": "error",
                "message": "advance_amount cannot be negative",
                "status_code": 400
            }

        if advance_amount > total_amount:
            return {
                "status": "error",
                "message": f"advance_amount ({advance_amount}) cannot exceed total ({total_amount})",
                "status_code": 400
            }

        # Build payment details
        remaining_amount = _calculate_remaining(total_amount, advance_amount)
        payment_status = _get_payment_status(payment_mode, advance_amount, total_amount)

        # Get existing transactions
        existing_details = current_record.get("payment_details") or {}
        if isinstance(existing_details, str):
            existing_details = json.loads(existing_details)
        existing_transactions = existing_details.get("transactions", [])

        # Add new transaction if provided
        add_transaction = payment_data.get("add_transaction")
        if add_transaction:
            new_txn = {
                "date": add_transaction.get("date") or str(datetime.now().date()),
                "amount": float(add_transaction.get("amount")),
                "method": add_transaction.get("method") or payment_method,
                "reference": add_transaction.get("reference") or reference_number,
                "notes": add_transaction.get("notes")
            }
            existing_transactions.append(new_txn)

        # Build final payment_details
        payment_details = _build_payment_details(
            payment_mode=payment_mode,
            advance_amount=advance_amount,
            remaining_amount=remaining_amount,
            paid_amount=advance_amount,
            payment_date=payment_date,
            payment_method=payment_method,
            reference_number=reference_number,
            notes=notes,
            transactions=existing_transactions
        )

        # Update station_bilty_summary
        update_data = {
            "payment_details": payment_details,
            "payment_status": payment_status,
            "advance_amount": advance_amount,
            "remaining_amount": remaining_amount
        }

        update_resp = sb.table("station_bilty_summary").update(
            update_data
        ).eq("gr_no", gr_no).execute()

        if not update_resp.data:
            return {
                "status": "error",
                "message": "Failed to update payment details",
                "status_code": 500
            }

        return {
            "status": "success",
            "gr_no": gr_no,
            "payment_details": payment_details,
            "payment_status": payment_status,
            "advance_amount": advance_amount,
            "remaining_amount": remaining_amount
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "status_code": 500
        }


def get_bilty_payment_details(bilty_id: str) -> dict:
    """Get payment details for a specific bilty."""
    try:
        sb = get_supabase()

        resp = sb.table("bilty").select(
            "id, gr_no, total, payment_details, payment_status, advance_amount, remaining_amount"
        ).eq("id", bilty_id).single().execute()

        if not resp.data:
            return {
                "status": "error",
                "message": "Bilty not found",
                "status_code": 404
            }

        bilty = resp.data
        return {
            "status": "success",
            "bilty_id": bilty["id"],
            "gr_no": bilty["gr_no"],
            "total_amount": float(bilty.get("total") or 0),
            "payment_status": bilty.get("payment_status"),
            "advance_amount": float(bilty.get("advance_amount") or 0),
            "remaining_amount": float(bilty.get("remaining_amount") or 0),
            "payment_details": bilty.get("payment_details")
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "status_code": 500
        }


def get_station_bilty_payment_details(gr_no: str) -> dict:
    """Get payment details for a station_bilty_summary."""
    try:
        sb = get_supabase()

        resp = sb.table("station_bilty_summary").select(
            "gr_no, amount, payment_details, payment_status, advance_amount, remaining_amount"
        ).eq("gr_no", gr_no).single().execute()

        if not resp.data:
            return {
                "status": "error",
                "message": "Station bilty not found",
                "status_code": 404
            }

        record = resp.data
        return {
            "status": "success",
            "gr_no": record["gr_no"],
            "total_amount": float(record.get("amount") or 0),
            "payment_status": record.get("payment_status"),
            "advance_amount": float(record.get("advance_amount") or 0),
            "remaining_amount": float(record.get("remaining_amount") or 0),
            "payment_details": record.get("payment_details")
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "status_code": 500
        }
