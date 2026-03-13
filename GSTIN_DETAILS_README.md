# GSTIN Details API

## Overview
Retrieves GSTIN details (trade name, legal name, address, state, status, etc.) from the NIC E-Way Bill system via the Masters India API.

## Endpoint

```
GET /api/gstin-details?userGstin=<USER_GSTIN>&gstin=<LOOKUP_GSTIN>
```

## Query Parameters

| Parameter   | Required | Description                         |
|-------------|----------|-------------------------------------|
| `userGstin` | Yes      | Logged-in user's GSTIN              |
| `gstin`     | Yes      | GSTIN number to look up             |

## Example Request

```bash
curl "https://your-server/api/gstin-details?userGstin=09COVPS5556J1ZT&gstin=05AAAAU6537D1ZO"
```

## Success Response (200)

```json
{
  "status": "success",
  "message": "GSTIN details retrieved successfully",
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
  "message": "325: Could not retrieve data",
  "nic_code": "325",
  "data": { "...full Masters India response..." }
}
```

## Files

| File                      | Purpose                      |
|---------------------------|------------------------------|
| `gstin_details_service.py`| Service logic & API call     |
| `app.py`                  | Flask route `/api/gstin-details` |
