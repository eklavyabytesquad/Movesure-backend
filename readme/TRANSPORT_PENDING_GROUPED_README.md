# Grouped Transport Pending Bilties API (by GSTIN)

Returns all bilties — across **every transport** — that are missing `pohonch_no` **OR** `bilty_number` in the `bilty_wise_kaat` table, **for challans dispatched between two dates**.  
Results are grouped: **GSTIN → transport_names[] → challan → serial-ordered bilties**.

---

## Endpoint

```
POST /api/bilty/transport-pending-grouped
```

### Body (JSON)
```json
{
  "dispatch_date_from": "2026-04-01",
  "dispatch_date_to":   "2026-04-23"
}
```
- Both fields required. Use `YYYY-MM-DD` or ISO date format.

---

## What "pending" means

A bilty row in `bilty_wise_kaat` is considered **pending** if:

| Condition | Meaning |
|-----------|---------|
| `pohonch_no` is `null` | Pohonch number not yet assigned |
| `bilty_number` is `null` | Bilty number not yet assigned |
| **Either** of the above | Row is included |

Rows where **both** `pohonch_no` and `bilty_number` are filled are **excluded**.

---

## Grouping Logic

- All transports with the **same GSTIN** are grouped together (e.g. all "ANKUR CARRIER" entries with same GSTIN).
- Each group shows:
  - `gst_number` (GSTIN)
  - `transport_names`: all transport names under this GSTIN
  - `city_names`: all cities under this GSTIN
  - `mob_numbers`: all mobile numbers under this GSTIN
  - `is_prior`: true if any transport in group is priority
  - `challans`: list of challans with pending bilties

---

## Response Structure

```json
{
  "status": "success",
  "total_groups": 5,
  "total_bilties": 120,
  "groups": [
    {
      "gst_number": "09ABCDE1234F1Z5",
      "transport_names": ["ANKUR CARRIER", "ANKUR CARRIERS"],
      "city_names": ["KANPUR", "LUCKNOW"],
      "mob_numbers": ["9876543210"],
      "is_prior": true,
      "total_bilties": 28,
      "total_challans": 3,
      "challans": [
        {
          "challan_no":   "0235",
          "bilty_count":  10,
          "total_weight": 520.5,
          "total_amount": 25000.0,
          "total_kaat":   1800.0,
          "bilties": [
            {
              "serial":         1,
              "gr_no":          "A09001",
              "challan_no":     "0235",
              "pohonch_no":     null,
              "bilty_number":   null,
              "station":        "KANPUR",
              "no_of_pkg":      3,
              "weight":         55.5,
              "pvt_marks":      "XYZ/123",
              "amount":         2500.0,
              "freight_amount": 2200.0,
              "kaat":           180.0,
              "pf":             20.0,
              "dd_chrg":        0.0,
              "consignor_name": "ABC TRADERS",
              "consignee_name": "DEF STORES",
              "payment_mode":   "TO-PAY",
              "delivery_type":  "godown",
              "bilty_date":     "2026-04-10",
              "transport_id":   "...",
              "transport_name": "ANKUR CARRIER",
              "city_name":      "KANPUR"
            }
          ]
        }
      ]
    }
  ]
}
```

---

## Field Reference

### Top-level

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | `"success"` or `"error"` |
| `total_groups` | int | Number of GSTIN groups with at least one pending bilty |
| `total_bilties` | int | Total pending bilty count across all groups |
| `groups` | array | GSTIN group objects (sorted by GSTIN) |

### GSTIN group object

| Field | Type | Description |
|-------|------|-------------|
| `gst_number` | string | GSTIN |
| `transport_names` | array | All transport names under this GSTIN |
| `city_names` | array | All cities under this GSTIN |
| `mob_numbers` | array | All mobile numbers under this GSTIN |
| `is_prior` | bool | True if any transport in group is priority |
| `total_bilties` | int | Pending bilties for this group |
| `total_challans` | int | Number of challans with pending bilties |
| `challans` | array | Challan objects (sorted by challan_no) |

### Challan object

| Field | Type | Description |
|-------|------|-------------|
| `challan_no` | string | Challan number (`"NO_CHALLAN"` if not assigned) |
| `bilty_count` | int | Number of pending bilties in this challan |
| `total_weight` | float | Sum of weights |
| `total_amount` | float | Sum of bilty amounts |
| `total_kaat` | float | Sum of kaat charges |
| `bilties` | array | Bilty objects (sorted by gr_no, serial starts at 1) |

### Bilty object

| Field | Type | Description |
|-------|------|-------------|
| `serial` | int | 1-based serial number within the challan |
| `gr_no` | string | GR number |
| `challan_no` | string\|null | Challan number |
| `pohonch_no` | string\|null | Pohonch number (null = not assigned) |
| `bilty_number` | string\|null | Bilty number (null = not assigned) |
| `station` | string | Destination station name (from `station_bilty_summary`) |
| `no_of_pkg` | int | Number of packages |
| `weight` | float | Weight in kg |
| `pvt_marks` | string | Private marks |
| `amount` | float | Total bilty amount |
| `freight_amount` | float | Freight amount |
| `kaat` | float | Kaat deduction |
| `pf` | float | PF charge |
| `dd_chrg` | float | DD charge |
| `consignor_name` | string | Consignor name |
| `consignee_name` | string | Consignee name |
| `payment_mode` | string | `PAID`, `TO-PAY`, etc. |
| `delivery_type` | string | `godown`, `door`, `door-delivery` |
| `bilty_date` | string | Bilty date (YYYY-MM-DD) |
| `transport_id` | string | Transport id for this bilty |
| `transport_name` | string | Transport name for this bilty |
| `city_name` | string | City for this bilty |

---

## Sorting Rules

| Level | Sort By |
|-------|---------|
| GSTIN groups | `gst_number` A→Z |
| Transport names | A→Z |
| Challans | `challan_no` A→Z |
| Bilties | `gr_no` A→Z |

---

## Error Response

```json
{
  "status": "error",
  "message": "Description of what went wrong"
}
```

---

## Example Usage (frontend)

```js
const res = await fetch('/api/bilty/transport-pending-grouped', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    dispatch_date_from: '2026-04-01',
    dispatch_date_to: '2026-04-23'
  })
});
const data = await res.json();

// data.groups → array of GSTIN groups
// data.groups[0].challans → challans for first GSTIN group
// data.groups[0].challans[0].bilties → bilties (with .serial)
```

---

## Data Sources

| Data | Table |
|------|-------|
| Pending filter | `bilty_wise_kaat` (pohonch_no / bilty_number null check) |
| Challan filter | `challan_details` (dispatch_date) |
| Transport details | `transports` |
| Packages, weight, freight, pvt_marks | `bilty` |
| Station name | `station_bilty_summary` |
