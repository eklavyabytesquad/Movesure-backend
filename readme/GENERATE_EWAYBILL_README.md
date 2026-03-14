# Generate E-Way Bill API

## Endpoint

```
POST /api/generate-ewaybill
Content-Type: application/json
```

> JWT auth is handled automatically by the backend. No token needed from frontend.

---

## Quick Start (Frontend)

```javascript
const response = await fetch("https://your-backend-url/api/generate-ewaybill", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(payload),
});
const result = await response.json();

if (result.status === "success") {
  console.log("EWB No:", result.ewayBillNo);
  console.log("PDF:", result.url);
} else {
  // result.errors is an array of human-readable error strings
  console.log("Errors:", result.errors);
}
```

---

## Request Body

### Required Fields

| Field | Type | Rules | Example |
|---|---|---|---|
| `userGstin` | string | **15 chars**, valid GSTIN. This is your **logged-in user's GSTIN** (default: `09COVPS5556J1ZT`) | `"09COVPS5556J1ZT"` |
| `supply_type` | string | `"outward"` or `"inward"` | `"outward"` |
| `sub_supply_type` | string | e.g. `"Supply"` | `"Supply"` |
| `document_type` | string | One of: `Tax Invoice`, `Bill of Supply`, `Bill of Entry`, `Delivery Challan`, `Credit Note`, `Others` | `"Tax Invoice"` |
| `document_number` | string | **Max 16 chars**, only `A-Z a-z 0-9 / -` allowed | `"MLMC/25-26/T001"` |
| `document_date` | string | `dd/mm/yyyy` format, must be <= today | `"14/03/2026"` |
| `gstin_of_consignor` | string | Valid 15-char GSTIN, or `"URP"` for unregistered | `"09AABFM8846M1ZM"` |
| `gstin_of_consignee` | string | Valid 15-char GSTIN, or `"URP"` for unregistered | `"09ASMPS6146H1Z5"` |
| `pincode_of_consignor` | number | Exactly **6 digits** | `202001` |
| `pincode_of_consignee` | number | Exactly **6 digits** | `211003` |
| `state_of_consignor` | string | Full state name | `"UTTAR PRADESH"` |
| `state_of_supply` | string | Full state name | `"UTTAR PRADESH"` |
| `taxable_amount` | number | Total taxable amount across all items | `303239.75` |
| `total_invoice_value` | number | Final invoice total | `357823` |
| `transportation_mode` | string | One of: `Road`, `Rail`, `Air`, `Ship`, `In Transit` | `"Road"` |
| `transportation_distance` | string/number | **0-4000** km. Pass `"0"` for auto-calculate | `"563"` |
| `itemList` | array | **1 to 250** items | See below |

### Optional Fields

| Field | Type | Example |
|---|---|---|
| `sub_supply_description` | string | `""` |
| `legal_name_of_consignor` | string | `"MODERN LOCK MFG. CO."` |
| `address1_of_consignor` | string | `"KOTHI NO.1 BANNA DEVI"` |
| `address2_of_consignor` | string | `""` |
| `place_of_consignor` | string | `"ALIGARH"` |
| `actual_from_state_name` | string | `"UTTAR PRADESH"` |
| `legal_name_of_consignee` | string | `"M.A ENTERPRISES"` |
| `address1_of_consignee` | string | `"127, DONDIPUR ALLAHABAD"` |
| `address2_of_consignee` | string | `""` |
| `place_of_consignee` | string | `"ALLAHABAD"` |
| `actual_to_state_name` | string | `"UTTAR PRADESH"` |
| `transaction_type` | number | `1`=Regular, `2`=Bill To/Ship To, `3`=Bill From/Dispatch From, `4`=Combination |
| `cgst_amount` | number | Required for **intra-state** | `27291.59` |
| `sgst_amount` | number | Required for **intra-state** | `27291.59` |
| `igst_amount` | number | Required for **inter-state** | `0` |
| `cess_amount` | number | | `0` |
| `cess_nonadvol_value` | number | | `0` |
| `other_value` | number | Other charges (+/-) | `0.07` |
| `transporter_id` | string | Transporter GSTIN | `"09COVPS5556J1ZT"` |
| `transporter_name` | string | | `"S S TRANSPORT CORPORATION"` |
| `transporter_document_number` | string | **Required for Rail/Air/Ship** | `""` |
| `transporter_document_date` | string | `dd/mm/yyyy` | `""` |
| `vehicle_number` | string | **Required for Road mode**. Format: `UP81CT9947` or `TMXXXXXX` (temp) | `"UP81CT9947"` |
| `vehicle_type` | string | `"Regular"` or `"ODC"` | `"Regular"` |
| `generate_status` | number | `1` | `1` |
| `data_source` | string | | `"erp"` |

### Item Fields (`itemList[]`)

| Field | Type | Required | Rules | Example |
|---|---|---|---|---|
| `product_name` | string | Yes | Auto-fills from `product_description` if empty | `"DOVE CAB HDL"` |
| `product_description` | string | No | | `"DOVE CAB HDL ROSE GOLD 8"` |
| `hsn_code` | string | Yes | **4-8 digits**, numeric only | `"83024110"` |
| `quantity` | number | Yes | Must be > 0 | `36` |
| `unit_of_product` | string | Yes | e.g. `PCS`, `BOX`, `KGS`, `NOS` | `"PCS"` |
| `cgst_rate` | number | Yes | | `9` |
| `sgst_rate` | number | Yes | | `9` |
| `igst_rate` | number | Yes | | `0` |
| `cess_rate` | number | No | | `0` |
| `cessNonAdvol` | number | No | | `0` |
| `taxable_amount` | number | Yes | | `2745` |

---

## Sample Request (Minimal)

```json
{
  "userGstin": "09COVPS5556J1ZT",
  "supply_type": "outward",
  "sub_supply_type": "Supply",
  "document_type": "Tax Invoice",
  "document_number": "INV/25-26/001",
  "document_date": "14/03/2026",
  "gstin_of_consignor": "09AABFM8846M1ZM",
  "legal_name_of_consignor": "MODERN LOCK MFG. CO.",
  "address1_of_consignor": "KOTHI NO.1 BANNA DEVI, G.T.ROAD, ALIGARH",
  "place_of_consignor": "ALIGARH",
  "pincode_of_consignor": 202001,
  "state_of_consignor": "UTTAR PRADESH",
  "actual_from_state_name": "UTTAR PRADESH",
  "gstin_of_consignee": "09ASMPS6146H1Z5",
  "legal_name_of_consignee": "M.A ENTERPRISES",
  "address1_of_consignee": "127, DONDIPUR ALLAHABAD",
  "place_of_consignee": "ALLAHABAD",
  "pincode_of_consignee": 211003,
  "state_of_supply": "UTTAR PRADESH",
  "actual_to_state_name": "UTTAR PRADESH",
  "transaction_type": 1,
  "taxable_amount": 500,
  "cgst_amount": 45,
  "sgst_amount": 45,
  "igst_amount": 0,
  "cess_amount": 0,
  "other_value": 0,
  "total_invoice_value": 590,
  "transporter_id": "09COVPS5556J1ZT",
  "transporter_name": "S S TRANSPORT CORPORATION",
  "transportation_mode": "Road",
  "transportation_distance": "563",
  "vehicle_number": "UP81CT9947",
  "vehicle_type": "Regular",
  "generate_status": 1,
  "data_source": "erp",
  "itemList": [
    {
      "product_name": "DOVE CAB HDL ROSE GOLD 8",
      "product_description": "DOVE CAB HDL ROSE GOLD 8",
      "hsn_code": "83024110",
      "quantity": 36,
      "unit_of_product": "PCS",
      "cgst_rate": 9,
      "sgst_rate": 9,
      "igst_rate": 0,
      "cess_rate": 0,
      "cessNonAdvol": 0,
      "taxable_amount": 500
    }
  ]
}
```

---

## Success Response (200)

```json
{
  "status": "success",
  "message": "E-Way Bill generated successfully",
  "ewayBillNo": 461698598183,
  "ewayBillDate": "14/03/2026 01:43:00 PM",
  "validUpto": "17/03/2026 11:59:00 PM",
  "alert": "",
  "url": "https://router.mastersindia.co/api/v1/detailPrintPdf/xxxx/",
  "data": { "...full API response..." }
}
```

**Frontend usage:**
```javascript
if (result.status === "success") {
  showSuccess(`E-Way Bill ${result.ewayBillNo} created!`);
  // Open PDF in new tab
  window.open(result.url, "_blank");
}
```

---

## Validation Error Response (400)

When your payload has issues, the backend catches them **before** calling the API and returns clear error messages:

```json
{
  "status": "error",
  "message": "Validation failed",
  "errors": [
    "document_number: Max 16 characters allowed, you provided 20 characters ('THIS-IS-WAY-TOO-LONG')",
    "vehicle_number: Invalid format 'ABC'. Use format like KA12BL4567 or TMXXXXXX for temp"
  ],
  "error_type": "validation"
}
```

**Frontend usage:**
```javascript
if (result.status === "error") {
  if (result.errors) {
    // Show each validation error to user
    result.errors.forEach((err) => showFieldError(err));
  } else {
    showError(result.message);
  }
}
```

---

## API Error Response (from Government/MastersIndia)

When payload passes local validation but the government API rejects it:

```json
{
  "status": "error",
  "message": "E-way bill(s) are already generated for the same document number, you cannot generate again on same document number",
  "nic_code": "",
  "data": { "...full API response..." }
}
```

---

## Validation Rules Summary

### document_number (IMPORTANT)
| Rule | Detail |
|---|---|
| Max length | **16 characters** |
| Allowed chars | `A-Z a-z 0-9 / -` only |
| Uniqueness | One EWB per document number per consignor. Cannot reuse. |

### GSTIN Fields
| Rule | Detail |
|---|---|
| Format | 15 chars: `09COVPS5556J1ZT` pattern |
| Unregistered | Pass `"URP"` for unregistered parties |
| userGstin | Must be **your account's GSTIN** (mapped in MastersIndia) |

### Transport
| Mode | vehicle_number | transporter_document_number |
|---|---|---|
| **Road** | **Required** | Optional |
| **Rail** | Optional | **Required** |
| **Air** | Optional | **Required** |
| **Ship** | Optional | **Required** |
| **In Transit** | Optional | Optional |

### Vehicle Number Format
- Standard: `KA12BL4567`, `UP81CT9947`
- Temporary: `TMXXXXXX`

### Amounts
- `taxable + cgst + sgst + igst + cess + other + cessNonAdvol` must be **<= total_invoice_value** (Rs.2 grace)
- **Intra-state** (same state): use `cgst_rate` / `sgst_rate`
- **Inter-state** (different states): use `igst_rate`

### Distance
- Range: **0 to 4000** km
- Pass `0` to let the system auto-calculate from pincodes
- Same pincode: max 100 km (300 km for Line Sales)

### Items
- Min: **1 item**, Max: **250 items**
- `hsn_code`: 4-8 digits, numeric only. At least one HSN code for goods required.
- `quantity`: must be > 0
- `product_name`: auto-fills from `product_description` if left empty

---

## Common Errors and How to Fix

| Error Message | Cause | Fix |
|---|---|---|
| `document_number: Max 16 characters` | Doc number too long | Shorten to 16 chars or less |
| `GSTIN does not exist / not mapped` | userGstin not linked to your account | Use your default GSTIN: `09COVPS5556J1ZT` |
| `already generated for same document number` | Duplicate doc number | Use a unique document number |
| `Invalid Supplier ship from State Code for the given pincode` | Pincode doesn't match the state | Check pincode-to-state mapping |
| `vehicle_number: Required when transportation_mode is 'Road'` | Missing vehicle for Road mode | Add `vehicle_number` field |
| `Amount mismatch` | Sum of amounts > total_invoice_value | Recalculate totals |
| `hsn_code must be 4-8 digits` | Invalid HSN | Use valid numeric HSN code |

---

## Frontend Form Field Mapping

| Form Field | API Field | Notes |
|---|---|---|
| User GSTIN (hidden/default) | `userGstin` | Default: `09COVPS5556J1ZT` |
| Supply Type dropdown | `supply_type` | `outward` / `inward` |
| Document Type dropdown | `document_type` | See allowed values above |
| Invoice Number input | `document_number` | Max 16 chars! |
| Invoice Date picker | `document_date` | Send as `dd/mm/yyyy` string |
| From GSTIN | `gstin_of_consignor` | |
| From Name | `legal_name_of_consignor` | |
| From Address 1 | `address1_of_consignor` | |
| From Address 2 | `address2_of_consignor` | |
| From Place | `place_of_consignor` | |
| From Pincode | `pincode_of_consignor` | Number, 6 digits |
| From State | `state_of_consignor` | Full name e.g. `UTTAR PRADESH` |
| To GSTIN | `gstin_of_consignee` | |
| To Name | `legal_name_of_consignee` | |
| To Address 1 | `address1_of_consignee` | |
| To Address 2 | `address2_of_consignee` | |
| To Place | `place_of_consignee` | |
| To Pincode | `pincode_of_consignee` | Number, 6 digits |
| To State | `state_of_supply` | Full name |
| Transport Mode dropdown | `transportation_mode` | `Road` / `Rail` / `Air` / `Ship` / `In Transit` |
| Distance input | `transportation_distance` | String, 0-4000 |
| Vehicle Number input | `vehicle_number` | Required for Road |
| Vehicle Type dropdown | `vehicle_type` | `Regular` / `ODC` |
| Transporter GSTIN | `transporter_id` | |
| Transporter Name | `transporter_name` | |
| Item rows | `itemList[]` | Each row = 1 item object |
