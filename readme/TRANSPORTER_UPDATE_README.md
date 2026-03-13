# Transporter Update API

## Endpoint

**POST** `/api/transporter-update`

Updates the transporter ID assigned to an active e-way bill. On success, also returns the PDF URL for printing.

Upstream MastersIndia API: `POST https://prod-api.mastersindia.co/api/v1/transporterIdUpdate/`

---

## Request Body

```json
{
  "user_gstin": "09COVPS5556J1ZT",
  "eway_bill_number": "491695305121",
  "transporter_id": "09AVKPJ3682J1Z2",
  "transporter_name": "DEEP PRIYAG"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `user_gstin` | string | Yes | GSTIN of the logged-in user (transporter or generator) |
| `eway_bill_number` | string / int | Yes | E-way bill number to update |
| `transporter_id` | string | Yes | New transporter's GSTIN |
| `transporter_name` | string | Yes | New transporter's name |

---

## Responses

### ✅ Success (200)

```json
{
  "status": "success",
  "message": "Transporter ID updated successfully",
  "eway_bill_number": "491695305121",
  "transporter_id": "09AVKPJ3682J1Z2",
  "update_date": "13/03/2026 01:43:00 AM",
  "pdf_url": "https://router.mastersindia.co/api/v1/detailPrintPdf/amFuX21hcl8yMDI1LTI2-69afd934b7a96f577e22498a/",
  "results": {
    "message": {
      "ewayBillNo": "491695305121",
      "transporterId": "09AVKPJ3682J1Z2",
      "transUpdateDate": "13/03/2026 01:43:00 AM",
      "error": false,
      "url": "https://router.mastersindia.co/api/v1/detailPrintPdf/..."
    },
    "status": "Success",
    "code": 200
  },
  "status_code": 200
}
```

| Field | Description |
|---|---|
| `eway_bill_number` | Confirmed EWB number |
| `transporter_id` | Updated transporter GSTIN |
| `update_date` | Timestamp of the update |
| `pdf_url` | Direct link to print the e-way bill PDF |

---

### ❌ NIC / Business Rule Error (422)

These are errors returned by the NIC/MastersIndia API due to business rule violations. The raw error string from NIC (e.g. `"338: You cannot update transporter..."`) is parsed into structured fields.

**Example — Transporter already entered Part B:**
```json
{
  "status": "error",
  "message": "You cannot update transporter details, as the current tranporter is already entered Part B details of the eway bill",
  "error_code": "338",
  "error_description": "You cannot update transporter details, as the current tranporter is already entered Part B details of the eway bill",
  "raw_error": "338: You cannot update transporter details, as the current tranporter is already entered Part B details of the eway bill",
  "results": { ... },
  "status_code": 422
}
```

**Example — Invalid EWB number:**
```json
{
  "status": "error",
  "message": "Invalid eway bill number",
  "error_code": "301",
  "error_description": "Invalid eway bill number",
  "raw_error": "301: Invalid eway bill number",
  "results": { ... },
  "status_code": 422
}
```

**Example — EWB already cancelled:**
```json
{
  "status": "error",
  "message": "Eway bill is already cancelled",
  "error_code": "312",
  "error_description": "Eway bill is already cancelled",
  "raw_error": "312: Eway bill is already cancelled",
  "results": { ... },
  "status_code": 422
}
```

| Field | Description |
|---|---|
| `status` | Always `"error"` |
| `message` | Human-readable error (clean, no NIC code prefix) |
| `error_code` | NIC error code (e.g. `"338"`) — use this for UI display or lookup |
| `error_description` | Same as `message` — full description |
| `raw_error` | Original raw string from NIC API (e.g. `"338: You cannot..."`) |

---

### ❌ Missing Fields (400)

```json
{
  "status": "error",
  "message": "Missing required fields: transporter_id, transporter_name"
}
```

---

### ❌ Auth Error (503)

```json
{
  "status": "error",
  "message": "Authentication failed. Unable to obtain valid JWT token."
}
```

---

### ❌ Timeout (408)

```json
{
  "status": "error",
  "message": "Request timed out. Please try again."
}
```

---

## Common NIC Error Codes

| Code | Description |
|---|---|
| `301` | Invalid eway bill number |
| `312` | Eway bill is already cancelled |
| `338` | Cannot update transporter — current transporter already entered Part B |
| `362` | Transporter document date cannot be earlier than invoice date |
| `371` | Invalid GSTIN for transporter |

---

## Example Request (curl)

```bash
curl -X POST http://localhost:5000/api/transporter-update \
  -H "Content-Type: application/json" \
  -d '{
    "user_gstin": "09COVPS5556J1ZT",
    "eway_bill_number": "491695305121",
    "transporter_id": "09AVKPJ3682J1Z2",
    "transporter_name": "DEEP PRIYAG"
  }'
```
