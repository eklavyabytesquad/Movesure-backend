# Transporter Details API

## Overview
Retrieves transporter details (trade name, legal name, address, state, status, etc.) from the NIC E-Way Bill system via the Masters India API.

> **Note:** This API uses the same `action=GetGSTINDetails` as the GSTIN Details API (#13). The NIC E-Way Bill system does not expose a separate "GetTransporterDetails" action — transporter info is fetched via the same GSTIN lookup. The difference is purely semantic: this endpoint is intended for looking up transporter GSTINs specifically. Failed lookups return NIC error code `328` ("Could not retrieve transporter details from gstin").

## Endpoint

```
GET /api/transporter-details?userGstin=<USER_GSTIN>&gstin=<TRANSPORTER_GSTIN>
```

## Query Parameters

| Parameter   | Required | Description                              |
|-------------|----------|------------------------------------------|
| `userGstin` | Yes      | Logged-in user's GSTIN                   |
| `gstin`     | Yes      | Transporter's GSTIN number to look up    |

## Example Request

```bash
curl "https://your-server/api/transporter-details?userGstin=09COVPS5556J1ZT&gstin=05AAAAU6537D1ZO"
```

## Success Response (200)

```json
{
  "status": "success",
  "message": "Transporter details retrieved successfully",
  "gstin_of_taxpayer": "05AAAAU6537D1ZO",
  "trade_name": "M/S UTTARAYAN CO-OPERATIVE FOR RENEWABLE ENERGY",
  "legal_name_of_business": "UTTARAYAN CO-OPERATIVE FOR RENEWABLE ENERGY",
  "address1": "UCREPeelikothi , Near the nainital bank ltd,Kaladhungi Road",
  "address2": "263139Peelikothi",
  "state_name": "UTTARAKHAND",
  "pincode": "263139",
  "taxpayer_type": "REG",
  "taxpayer_status": "ACT",
  "block_status": "",
  "data": { "...full Masters India response..." }
}
```

## Error Response

```json
{
  "status": "error",
  "message": "328: Could not retrieve transporter details from gstin",
  "nic_code": "328",
  "data": { "...full Masters India response..." }
}
```

## Files

| File                                  | Purpose                               |
|---------------------------------------|---------------------------------------|
| `services/transporter_details_service.py` | Service logic & API call              |
| `app.py`                              | Flask route `/api/transporter-details`|
