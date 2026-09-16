"""
Consignee Rates Service
Fetches consignee-specific rates from consignee_bilty_profile
and default rates from the rates table.
Uses parallel queries for speed.
"""
from concurrent.futures import as_completed
from services.supabase_client import get_supabase
from services.thread_pool import shared_pool


def get_consignee_rates(consignee_id: str) -> dict:
    """
    Fetch all active rate profiles for a consignee.
    Returns destination-wise rate, labour, DD charges, etc.
    """
    try:
        sb = get_supabase()

        data = (
            sb.table("consignee_bilty_profile")
            .select(
                "id, consignee_id, destination_station_id, city_code, city_name, "
                "transport_name, transport_gst, rate, rate_unit, minimum_weight_kg, "
                "labour_rate, labour_unit, dd_charge_per_kg, dd_charge_per_nag, "
                "receiving_slip_charge, bilty_charge, is_no_charge, "
                "effective_from, effective_to, is_active, "
                "dd_print_charge_per_kg, dd_print_charge_per_nag, "
                "is_toll_tax_applicable, toll_tax_amount, freight_minimum_amount"
            )
            .eq("consignee_id", consignee_id)
            .eq("is_active", True)
            .order("city_name")
            .execute()
        ).data or []

        # Build lookup by destination city for quick frontend access
        by_city = {}
        for r in data:
            cid = r.get("destination_station_id")
            if cid:
                by_city.setdefault(cid, []).append(r)

        return {
            "status": "success",
            "data": {
                "rates": data,
                "rates_by_city": by_city,
                "count": len(data),
            },
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to fetch consignee rates: {str(e)}",
            "status_code": 500,
        }


def get_default_rates(branch_id: str) -> dict:
    """
    Fetch default rates (is_default=true) for a branch.
    These are city-wise base rates used when no consignee profile exists.
    """
    try:
        sb = get_supabase()

        data = (
            sb.table("rates")
            .select("id, branch_id, city_id, rate, is_default")
            .eq("branch_id", branch_id)
            .eq("is_default", True)
            .execute()
        ).data or []

        # Build lookup by city_id
        by_city = {r["city_id"]: r["rate"] for r in data}

        return {
            "status": "success",
            "data": {
                "rates": data,
                "rate_by_city_id": by_city,
                "count": len(data),
            },
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to fetch default rates: {str(e)}",
            "status_code": 500,
        }


DD_MINIMUM = 150.0


def calculate_dd_charge(
    consignee_id: str,
    destination_city_id: str,
    weight: float,
    no_of_pkg: int,
) -> dict:
    """
    Calculate the door-delivery charge for a bilty.

    Looks up the consignee's active profile row matching consignee_id +
    destination_station_id.  Applies:
      • per-kg  rate  when dd_charge_per_kg  > 0
      • per-nag rate  when dd_charge_per_nag > 0 (fallback)
      • hard minimum of 150 regardless of the calculated value

    Returns dd_charge, the basis used, and the raw per-unit rates so the
    frontend can display the breakdown.
    """
    try:
        sb = get_supabase()

        rows = (
            sb.table("consignee_bilty_profile")
            .select("id, dd_charge_per_kg, dd_charge_per_nag")
            .eq("consignee_id", consignee_id)
            .eq("destination_station_id", destination_city_id)
            .eq("is_active", True)
            .limit(1)
            .execute()
        ).data

        if not rows:
            return {
                "status": "success",
                "data": {
                    "dd_charge": DD_MINIMUM,
                    "raw_calculated": 0.0,
                    "basis": "minimum_no_profile",
                    "per_kg_rate": 0.0,
                    "per_nag_rate": 0.0,
                    "profile_id": None,
                },
            }

        profile = rows[0]
        per_kg = float(profile.get("dd_charge_per_kg") or 0)
        per_nag = float(profile.get("dd_charge_per_nag") or 0)

        if per_kg > 0:
            raw = per_kg * float(weight)
            basis = "per_kg"
        elif per_nag > 0:
            raw = per_nag * int(no_of_pkg)
            basis = "per_nag"
        else:
            raw = 0.0
            basis = "minimum"

        dd_charge = max(raw, DD_MINIMUM)

        return {
            "status": "success",
            "data": {
                "dd_charge": round(dd_charge, 2),
                "raw_calculated": round(raw, 2),
                "basis": basis,
                "per_kg_rate": per_kg,
                "per_nag_rate": per_nag,
                "profile_id": profile["id"],
            },
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to calculate DD charge: {str(e)}",
            "status_code": 500,
        }


def get_all_rates(consignee_id: str, branch_id: str) -> dict:
    """
    Fetch BOTH consignee-specific rates AND default branch rates in parallel.
    Frontend can fall back to default when no consignee profile exists for a city.
    """
    try:
        results = {}
        futures = {
            shared_pool.submit(get_consignee_rates, consignee_id): "consignee_rates",
            shared_pool.submit(get_default_rates, branch_id): "default_rates",
        }
        for future in as_completed(futures):
            key = futures[future]
            results[key] = future.result()

        # Check for errors
        for key, res in results.items():
            if res.get("status") != "success":
                return res

        return {
            "status": "success",
            "data": {
                "consignee_rates": results["consignee_rates"]["data"]["rates"],
                "consignee_rates_by_city": results["consignee_rates"]["data"]["rates_by_city"],
                "default_rates": results["default_rates"]["data"]["rates"],
                "default_rate_by_city_id": results["default_rates"]["data"]["rate_by_city_id"],
            },
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to fetch rates: {str(e)}",
            "status_code": 500,
        }
