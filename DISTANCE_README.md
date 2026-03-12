# Distance API

## Endpoint

**GET** `/api/distance`

Returns the distance (in km) between two Indian pincodes via the MastersIndia API.

---

## Query Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fromPincode` | string/int | Yes | Origin pincode (6 digits) |
| `toPincode` | string/int | Yes | Destination pincode (6 digits) |

---

## Example Request

```bash
curl "http://localhost:5000/api/distance?fromPincode=201301&toPincode=560093"
```

---

## Success Response (200)

```json
{
  "status": "success",
  "message": "Distance fetched successfully: 2145.456 km",
  "results": {
    "distance": 2145.456,
    "status": "Success",
    "code": 200
  },
  "status_code": 200
}
```

---

## Error Responses

**Invalid pincode (204)**

```json
{
  "status": "error",
  "message": "Invalid pin code",
  "results": {
    "message": "Invalid pin code",
    "status": "No Content",
    "code": 204
  },
  "status_code": 204
}
```

**Missing parameters (400)**

```json
{
  "status": "error",
  "message": "Missing required query parameters: fromPincode and toPincode"
}
```

---

## Notes

- Both pincodes must be valid 6-digit Indian pincodes.
- JWT authentication is handled automatically by the server middleware.
- Useful for validating `remaining_distance` when extending e-way bill validity.
