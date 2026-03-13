# Extend E-Way Bill Validity API

## Endpoint

**POST** `/api/extend-ewaybill`

Extends the validity of an existing e-way bill via the MastersIndia API.

---

## Request Body (JSON)

| Field | Type | Required | Description |
|---|---|---|---|
| `userGstin` | string | Yes | User's GSTIN |
| `eway_bill_number` | int | Yes | E-way bill number to extend |
| `vehicle_number` | string | Yes | Vehicle number (format: `KA12TR1234`) |
| `place_of_consignor` | string | Yes | Current location of consignment |
| `state_of_consignor` | string | Yes | State of current location |
| `remaining_distance` | int | Yes | Remaining km to destination (must not exceed original distance) |
| `transporter_document_number` | string | Conditional | Required for Rail/Air/Ship (modes 2,3,4). Optional for Road (mode 1) |
| `transporter_document_date` | string | No | Format: `DD/MM/YYYY` |
| `mode_of_transport` | string | Yes | `1`=Road, `2`=Rail, `3`=Air, `4`=Ship, `5`=In Transit |
| `extend_validity_reason` | string | Yes | Reason for extension (e.g., "Natural Calamity") |
| `extend_remarks` | string | Yes | Additional details (e.g., "Flood") |
| `consignment_status` | string | Yes | `M`=Moving (modes 1-4), `T`=In Transit (mode 5) |
| `from_pincode` | int | Yes | Pincode of current location |
| `transit_type` | string | Conditional | Only for mode 5: `R`=Road, `W`=Warehouse, `O`=Others. Blank for modes 1-4 |
| `address_line1` | string | Yes | Address line 1 |
| `address_line2` | string | Yes | Address line 2 |
| `address_line3` | string | Yes | Address line 3 |

---

## Validation Rules

| Rule | Details |
|---|---|
| **Modes 1-4** | `consignment_status` must be `"M"`, `transit_type` must be `""` |
| **Mode 5** | `consignment_status` must be `"T"`, `transit_type` must be `"R"`, `"W"`, or `"O"` |
| **Road (mode 1)** | `vehicle_number` is mandatory, `transporter_document_number` is optional |
| **Rail/Air/Ship (2,3,4)** | `transporter_document_number` is mandatory |
| **Vehicle format** | Must match standard format, e.g. `KA12TR1234` |
| **Distance** | `remaining_distance` must not exceed original distance on the e-way bill |
| **Timing window** | Can only extend between 8 hours before and 8 hours after expiry |
| **Authorization** | Only the current transporter can extend. If no transporter assigned, the generator can extend |

---

## Example Request

```bash
curl -X POST http://localhost:5000/api/extend-ewaybill \
  -H "Content-Type: application/json" \
  -d '{
    "userGstin": "05AAABB0639G1Z8",
    "eway_bill_number": 311003430463,
    "vehicle_number": "KA12TR1234",
    "place_of_consignor": "Dehradun",
    "state_of_consignor": "UTTARAKHAND",
    "remaining_distance": 10,
    "transporter_document_number": "123",
    "transporter_document_date": "25/06/2023",
    "mode_of_transport": "5",
    "extend_validity_reason": "Natural Calamity",
    "extend_remarks": "Flood",
    "consignment_status": "T",
    "from_pincode": 248001,
    "transit_type": "W",
    "address_line1": "HUBLI",
    "address_line2": "HUBLI",
    "address_line3": "HUBLI"
  }'
```

---

## Success Response (200)

```json
{
  "status": "success",
  "message": "E-Way Bill validity extended successfully",
  "results": {
    "message": {
      "ewayBillNo": "331009219156",
      "updatedDate": "20/09/2023 07:07:00 PM",
      "validUpto": "21/09/2023 11:59:00 PM",
      "error": false,
      "url": "https://sandb-api.mastersindia.co/api/v1/detailPrintPdf/..."
    },
    "status": "Success",
    "code": 200
  },
  "status_code": 200
}
```

---

## Error Response (204 / validation)

```json
{
  "status": "error",
  "message": "301: Invalid eway bill number",
  "results": {
    "message": "301: Invalid eway bill number",
    "status": "No Content",
    "code": 204,
    "nic_code": "301"
  },
  "status_code": 204
}
```

---

## Quick Reference: Transport Modes

| Code | Mode | `consignment_status` | `transit_type` | `vehicle_number` | `transporter_document_number` |
|---|---|---|---|---|---|
| 1 | Road | M | _(blank)_ | Required | Optional |
| 2 | Rail | M | _(blank)_ | Optional | Required |
| 3 | Air | M | _(blank)_ | Optional | Required |
| 4 | Ship | M | _(blank)_ | Optional | Required |
| 5 | In Transit | T | R / W / O | Optional | Optional |
