"""
Consignor Rates Service
Fetches consignor-specific rates from consignor_bilty_profile
and default rates from the rates table.
Uses parallel queries for speed.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from services.supabase_client import get_supabase


def get_consignor_rates(consignor_id: str) -> dict:
    """
    Fetch all active rate profiles for a consignor.
    Returns destination-wise rate, labour, DD charges, etc.
    """
    try:
        sb = get_supabase()

        data = (
            sb.table("consignor_bilty_profile")
            .select(
                "id, consignor_id, destination_station_id, city_code, city_name, "
                "transport_name, transport_gst, rate, rate_unit, minimum_weight_kg, "
                "labour_rate, labour_unit, dd_charge_per_kg, dd_charge_per_nag, "
                "receiving_slip_charge, bilty_charge, is_no_charge, "
                "effective_from, effective_to, is_active, "
                "dd_print_charge_per_kg, dd_print_charge_per_nag, "
                "is_toll_tax_applicable, toll_tax_amount, freight_minimum_amount"
            )
            .eq("consignor_id", consignor_id)
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
            "message": f"Failed to fetch consignor rates: {str(e)}",
            "status_code": 500,
        }


def get_default_rates(branch_id: str) -> dict:
    """
    Fetch default rates (is_default=true) for a branch.
    These are city-wise base rates used when no consignor profile exists.
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


def get_all_rates(consignor_id: str, branch_id: str) -> dict:
    """
    Fetch BOTH consignor-specific rates AND default branch rates in parallel.
    Frontend can fall back to default when no consignor profile exists for a city.
    """
    try:
        results = {}
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = {
                pool.submit(get_consignor_rates, consignor_id): "consignor_rates",
                pool.submit(get_default_rates, branch_id): "default_rates",
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
                "consignor_rates": results["consignor_rates"]["data"]["rates"],
                "consignor_rates_by_city": results["consignor_rates"]["data"]["rates_by_city"],
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
