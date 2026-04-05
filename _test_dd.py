import requests, json

BASE = "http://localhost:5000"
CID  = "22873c8c-e2af-4e08-bfc6-40864c477943"

print("=== GET /api/bilty/rates/consignor/{id} ===\n")
resp = requests.get(f"{BASE}/api/bilty/rates/consignor/{CID}")
body = resp.json()
data = body.get("data", {})
print(f"Status: {resp.status_code}")
print(f"Total rates: {data.get('count', 0)}")

for r in data.get("rates", [])[:5]:
    print(f"\nCity: {r.get('city_name','?')} ({r.get('city_code','?')})")
    print(f"  Transport: {r.get('transport_name','?')}")
    print(f"  DD charge/kg:       {r.get('dd_charge_per_kg')}")
    print(f"  DD charge/nag:      {r.get('dd_charge_per_nag')}")
    print(f"  DD PRINT charge/kg: {r.get('dd_print_charge_per_kg')}")
    print(f"  DD PRINT charge/nag:{r.get('dd_print_charge_per_nag')}")
    print(f"  Bilty charge:       {r.get('bilty_charge')}")
    print(f"  RS charge:          {r.get('receiving_slip_charge')}")
    print(f"  Toll: {r.get('is_toll_tax_applicable')} / Rs {r.get('toll_tax_amount')}")

print("\n\n=== rates_by_city sample (first 3 keys) ===\n")
by_city = data.get("rates_by_city", {})
for i, (city_id, rates_list) in enumerate(by_city.items()):
    if i >= 3:
        break
    rate = rates_list[0] if isinstance(rates_list, list) else rates_list
    print(f"  {city_id} -> {rate.get('city_name')}:")
    print(f"    dd_charge_per_kg:       {rate.get('dd_charge_per_kg')}")
    print(f"    dd_charge_per_nag:      {rate.get('dd_charge_per_nag')}")
    print(f"    dd_print_charge_per_kg: {rate.get('dd_print_charge_per_kg')}")
    print(f"    dd_print_charge_per_nag:{rate.get('dd_print_charge_per_nag')}")
