# Get E-Way Bill Details API

## Endpoint

**GET** `/api/ewaybill`

Fetches complete details of an e-way bill from the MastersIndia API using the EWB number and GSTIN.

---

## Query Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `eway_bill_number` | string | Yes | E-way bill number (e.g., `451693644839`) |
| `gstin` | string | Yes | GSTIN of the party (e.g., `09COVPS5556J1ZT`) |

---

## Example Request

```bash
curl "http://localhost:5000/api/ewaybill?eway_bill_number=451693644839&gstin=09COVPS5556J1ZT"
```

---

## Success Response (200)

The API returns a JSON object wrapping the full e-way bill data. Here's the complete structure:

```json
{
  "status": "success",
  "message": "E-Way Bill details retrieved successfully",
  "data": {
    "results": {
      "message": {
        "eway_bill_number": 451693644839,
        "eway_bill_date": "10/03/2026 02:11:00 PM",
        "generate_mode": "API",
        "userGstin": "09AABFM8846M1ZM",
        "supply_type": "OUTWARD",
        "sub_supply_type": "Supply",
        "document_type": "Tax Invoice",
        "document_number": "MLMC/25-26/4919",
        "document_date": "01/03/2026",

        "gstin_of_consignor": "09AABFM8846M1ZM",
        "legal_name_of_consignor": "MODERN LOCK MFG. CO.",
        "address1_of_consignor": "RAMSON HOUSE KOTHI NO.1 BANNA DEVI, G.T.ROAD,ALIGARH",
        "address2_of_consignor": "",
        "place_of_consignor": "ALIGARH",
        "pincode_of_consignor": 202001,
        "state_of_consignor": "UTTAR PRADESH",

        "gstin_of_consignee": "09ASMPS6146H1Z5",
        "legal_name_of_consignee": "M.A ENTERPRISES",
        "address1_of_consignee": "127, DONDIPUR ALLAHABAD",
        "address2_of_consignee": "",
        "place_of_consignee": "ALLAHABD",
        "pincode_of_consignee": 211003,
        "state_of_supply": "UTTAR PRADESH",

        "taxable_amount": 303239.75,
        "total_invoice_value": 357823.0,
        "cgst_amount": 27291.59,
        "sgst_amount": 27291.59,
        "igst_amount": 0.0,
        "cess_amount": 0.0,
        "other_value": 0.07,
        "cess_nonadvol_value": 0.0,

        "transporter_id": "09COVPS5556J1ZT",
        "transporter_name": "S S TRANSPORT CORPORATION",
        "eway_bill_status": "Active",
        "transportation_distance": 563,
        "number_of_valid_days": 3,
        "eway_bill_valid_date": "13/03/2026 11:59:00 PM",
        "extended_times": 0,
        "reject_status": "N",
        "vehicle_type": "regular",
        "actual_from_state_name": "UTTAR PRADESH",
        "actual_to_state_name": "UTTAR PRADESH",
        "transaction_type": "Regular",

        "itemList": [
          {
            "item_number": 1,
            "product_id": 0,
            "product_name": "",
            "product_description": "DOVE CAB HDL ROSE GOLD 8",
            "hsn_code": 83024110,
            "quantity": 36.0,
            "unit_of_product": "PCS",
            "cgst_rate": 9.0,
            "sgst_rate": 9.0,
            "igst_rate": 0,
            "cess_rate": 0.0,
            "cessNonAdvol": 0.0,
            "taxable_amount": 2745.0
          }
          // ... more items
        ],

        "VehiclListDetails": [
          {
            "update_mode": "API",
            "vehicle_number": "UP81CT9947",
            "place_of_consignor": "aligarh",
            "state_of_consignor": "UTTAR PRADESH",
            "tripshtNo": 4029554810,
            "userGstin": "09COVPS5556J1ZT",
            "vehicle_number_update_date": "10/03/2026 02:11:00 PM",
            "transportation_mode": "Road",
            "transporter_document_number": "",
            "transporter_document_date": "10/03/2026",
            "group_number": "0"
          }
        ]
      },
      "status": "Success",
      "code": 200
    }
  }
}
```

---

## JSON Structure Breakdown

### Top Level

| Field | Type | Description |
|---|---|---|
| `status` | string | `"success"` or `"error"` |
| `message` | string | Human-readable status message |
| `data.results.message` | object | The full e-way bill object (see below) |
| `data.results.status` | string | `"Success"` from upstream API |
| `data.results.code` | int | `200` on success |

### E-Way Bill Object (`data.results.message`)

| Field | Type | Description |
|---|---|---|
| `eway_bill_number` | int | E-way bill number |
| `eway_bill_date` | string | Generation date/time (`DD/MM/YYYY HH:MM:SS AM/PM`) |
| `generate_mode` | string | How it was generated (`API`, `Web`, etc.) |
| `userGstin` | string | Generator's GSTIN |
| `supply_type` | string | `OUTWARD` or `INWARD` |
| `sub_supply_type` | string | Sub-type (e.g., `Supply`, `Job Work`) |
| `document_type` | string | `Tax Invoice`, `Bill of Supply`, etc. |
| `document_number` | string | Invoice/document number |
| `document_date` | string | Document date (`DD/MM/YYYY`) |

### Consignor (From)

| Field | Type | Description |
|---|---|---|
| `gstin_of_consignor` | string | Consignor GSTIN |
| `legal_name_of_consignor` | string | Business name |
| `address1_of_consignor` | string | Address line 1 |
| `address2_of_consignor` | string | Address line 2 |
| `place_of_consignor` | string | City/place |
| `pincode_of_consignor` | int | Pincode |
| `state_of_consignor` | string | State name |

### Consignee (To)

| Field | Type | Description |
|---|---|---|
| `gstin_of_consignee` | string | Consignee GSTIN |
| `legal_name_of_consignee` | string | Business name |
| `address1_of_consignee` | string | Address line 1 |
| `address2_of_consignee` | string | Address line 2 |
| `place_of_consignee` | string | City/place |
| `pincode_of_consignee` | int | Pincode |
| `state_of_supply` | string | State of supply |

### Tax & Value

| Field | Type | Description |
|---|---|---|
| `taxable_amount` | float | Total taxable amount |
| `total_invoice_value` | float | Total invoice value (incl. tax) |
| `cgst_amount` | float | CGST amount |
| `sgst_amount` | float | SGST amount |
| `igst_amount` | float | IGST amount |
| `cess_amount` | float | Cess amount |
| `other_value` | float | Other charges |

### Transport & Validity

| Field | Type | Description |
|---|---|---|
| `transporter_id` | string | Transporter GSTIN |
| `transporter_name` | string | Transporter name |
| `eway_bill_status` | string | `Active`, `Cancelled`, `Expired` |
| `transportation_distance` | int | Distance in km |
| `number_of_valid_days` | int | Number of valid days |
| `eway_bill_valid_date` | string | Expiry date/time |
| `extended_times` | int | How many times validity was extended |
| `vehicle_type` | string | `regular` or `over_dimensional_cargo` |
| `transaction_type` | string | `Regular`, `Bill To - Ship To`, etc. |

### Item List (`itemList[]`)

| Field | Type | Description |
|---|---|---|
| `item_number` | int | Sequential item number |
| `product_description` | string | Product description |
| `hsn_code` | int | HSN code |
| `quantity` | float | Quantity |
| `unit_of_product` | string | Unit (e.g., `PCS`, `KGS`) |
| `cgst_rate` | float | CGST rate % |
| `sgst_rate` | float | SGST rate % |
| `igst_rate` | float | IGST rate % |
| `cess_rate` | float | Cess rate % |
| `taxable_amount` | float | Taxable amount for this item |

### Vehicle List (`VehiclListDetails[]`)

| Field | Type | Description |
|---|---|---|
| `vehicle_number` | string | Vehicle registration number |
| `place_of_consignor` | string | Place where vehicle was updated |
| `state_of_consignor` | string | State |
| `transportation_mode` | string | `Road`, `Rail`, `Air`, `Ship` |
| `vehicle_number_update_date` | string | When vehicle was assigned/updated |
| `transporter_document_number` | string | LR/RR/Airway bill number |
| `transporter_document_date` | string | Document date |
| `update_mode` | string | `API` or `Web` |
| `tripshtNo` | int | Trip sheet number |
| `userGstin` | string | GSTIN of the user who updated |

---

## Error Response

```json
{
  "status": "error",
  "message": "Failed to retrieve E-Way Bill details",
  "error": { ... },
  "status_code": 404
}
```

**Missing parameters (400):**

```json
{
  "status": "error",
  "message": "Missing required parameters: eway_bill_number and gstin"
}
```
