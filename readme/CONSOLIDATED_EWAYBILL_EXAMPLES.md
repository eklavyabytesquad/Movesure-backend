# 📦 Consolidated E-Way Bill - Complete Examples

## 🎯 What is a Consolidated E-Way Bill?

A **Consolidated E-Way Bill (CEWB)** is created when multiple e-way bills need to be transported together in a single vehicle. It's like a "master document" that groups individual e-way bills.

### When to use it:
- 🚛 Multiple shipments in one truck
- 📦 Different e-way bills, same vehicle
- 🛣️ Long-distance transport with multiple consignments

---

## 📋 API Endpoint Details

**URL:** `http://localhost:5000/api/consolidated-ewaybill`  
**Method:** `POST`  
**Content-Type:** `application/json`

---

## 📝 Complete Request Exampl

### Example 1: Multiple E-Way Bills Consolidation
```json
{
  "userGstin": "09COVPS5556J1ZT",
  "place_of_consignor": "Mumbai",
  "state_of_consignor": "Maharashtra",
  "vehicle_number": "MH12AB1234",
  "mode_of_transport": "1",
  "transporter_document_number": "TD002",
  "transporter_document_date": "07/10/2025",
  "data_source": "erp",
  "list_of_eway_bills": [
    {
      "eway_bill_number": "441629168735"
    },
    {
      "eway_bill_number": "451629889107"
    },
    {
      "eway_bill_number": "451629889108"
    },
    {
      "eway_bill_number": "461629889109"
    }
  ]
}
```

### ❌ WRONG FORMAT (Don't use this!)
```json
{
  "list_of_eway_bills": [
    "441629168735",
    "451629889107"
  ]
}
```
**This will give error:** "ensure this value has at least 12 characters"

### Example 2: Rail Transport
```json
{
  "userGstin": "09COVPS5556J1ZT",
  "place_of_consignor": "Kolkata",
  "state_of_consignor": "West Bengal",
  "vehicle_number": "WB19CD5678",
  "mode_of_transport": "2",
  "transporter_document_number": "RAIL123",
  "transporter_document_date": "07/10/2025",
  "data_source": "web",
  "list_of_eway_bills": [
    {
      "eway_bill_number": "441629168735"
    },
    {
      "eway_bill_number": "451629889107"
    }
  ]
}
```

### Example 3: Your Actual Request (28 E-Way Bills)
```json
{
  "userGstin": "09COVPS5556J1ZT",
  "place_of_consignor": "ALIGARH",
  "state_of_consignor": "Uttar Pradesh",
  "vehicle_number": "UP35BT7389",
  "mode_of_transport": "1",
  "transporter_document_number": "TD001",
  "transporter_document_date": "07/10/2025",
  "data_source": "erp",
  "list_of_eway_bills": [
    {"eway_bill_number": "401629433363"},
    {"eway_bill_number": "401630150547"},
    {"eway_bill_number": "421630187202"},
    {"eway_bill_number": "441629168735"},
    {"eway_bill_number": "481629314257"},
    {"eway_bill_number": "431629350566"},
    {"eway_bill_number": "421629275691"},
    {"eway_bill_number": "431629356759"},
    {"eway_bill_number": "401629296485"},
    {"eway_bill_number": "491629368299"},
    {"eway_bill_number": "411629933275"},
    {"eway_bill_number": "451629889107"},
    {"eway_bill_number": "451629890237"},
    {"eway_bill_number": "481630061652"},
    {"eway_bill_number": "441630111265"},
    {"eway_bill_number": "491630114115"},
    {"eway_bill_number": "441630051510"},
    {"eway_bill_number": "401630080640"},
    {"eway_bill_number": "451630077478"},
    {"eway_bill_number": "481629959003"},
    {"eway_bill_number": "451630076235"},
    {"eway_bill_number": "481630068202"},
    {"eway_bill_number": "441630134585"},
    {"eway_bill_number": "461630134057"},
    {"eway_bill_number": "441630134598"},
    {"eway_bill_number": "481630137867"},
    {"eway_bill_number": "471630137877"},
    {"eway_bill_number": "421629958987"}
  ]
}
```

---

## 📊 Field Descriptions

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `userGstin` | String | ✅ Yes | Your company's GSTIN number | `"09COVPS5556J1ZT"` |
| `place_of_consignor` | String | ✅ Yes | Starting location/city | `"Delhi"` |
| `state_of_consignor` | String | ✅ Yes | State name | `"Delhi"` |
| `vehicle_number` | String | ✅ Yes | Vehicle registration number | `"DL01AB1234"` |
| `mode_of_transport` | String | ✅ Yes | Transport mode (see below) | `"1"` |
| `transporter_document_number` | String | ✅ Yes | Your transport document number | `"TD001"` |
| `transporter_document_date` | String | ✅ Yes | Date in DD/MM/YYYY format | `"07/10/2025"` |
| `data_source` | String | ✅ Yes | Source of data | `"erp"` or `"web"` |
| `list_of_eway_bills` | Array | ✅ Yes | Array of objects with eway_bill_number | `[{"eway_bill_number": "123456"}]` |

---

## 🚗 Mode of Transport Values

| Value | Mode | Description |
|-------|------|-------------|
| `"1"` | Road | Truck, Tempo, Lorry |
| `"2"` | Rail | Train transport |
| `"3"` | Air | Flight, Cargo plane |
| `"4"` | Ship | Sea/River transport |

---

## ✅ Expected Success Response

```json
{
  "status": "success",
  "message": "Consolidated E-Way Bill created successfully",
  "data": {
    "consolidatedEwayBillNumber": "CEW123456789",
    "vehicleNumber": "DL01AB1234",
    "consolidatedDate": "07/10/2025",
    "validUpto": "2025-10-08T23:59:59",
    "ewayBills": [
      {
        "ewayBillNumber": "441629168735",
        "documentNumber": "DOC001",
        "fromGstin": "09COVPS5556J1ZT",
        "toGstin": "27AAAAA0000A1Z5"
      },
      {
        "ewayBillNumber": "451629889107",
        "documentNumber": "DOC002",
        "fromGstin": "09COVPS5556J1ZT",
        "toGstin": "29BBBBB0000B1Z6"
      }
    ],
    "totalEwayBills": 2
  }
}
```

---

## ❌ Common Error Responses

### Error 1: Missing Required Fields
```json
{
  "status": "error",
  "message": "Missing required fields: vehicle_number, list_of_eway_bills"
}
```

### Error 2: Invalid E-Way Bill
```json
{
  "status": "error",
  "message": "Failed to create consolidated e-way bill",
  "error": {
    "errorCode": "EWB_404",
    "errorMessage": "E-way bill 441629168735 not found or already consolidated"
  },
  "status_code": 400
}
```

### Error 3: Authentication Error
```json
{
  "status": "error",
  "message": "Failed to get authentication token"
}
```

---

## 🌐 How to Test in Chrome

### Method 1: Using test_api.html (Recommended)
1. Open `test_api.html` in Chrome
2. Scroll to "📦 2. Create Consolidated E-Way Bill" section
3. Fill in all fields
4. Click "🚀 Create Consolidated E-Way Bill"
5. See the response below

### Method 2: Using PowerShell (CORRECT FORMAT)
```powershell
$body = @{
    userGstin = "09COVPS5556J1ZT"
    place_of_consignor = "Delhi"
    state_of_consignor = "Delhi"
    vehicle_number = "DL01AB1234"
    mode_of_transport = "1"
    transporter_document_number = "TD001"
    transporter_document_date = "07/10/2025"
    data_source = "erp"
    list_of_eway_bills = @(
        @{eway_bill_number = "441629168735"},
        @{eway_bill_number = "451629889107"}
    )
} | ConvertTo-Json -Depth 5

Invoke-RestMethod -Uri "http://localhost:5000/api/consolidated-ewaybill" -Method Post -Body $body -ContentType "application/json"
```

**Note:** The service will **automatically convert** string arrays to the correct format, so you can also send:
```powershell
list_of_eway_bills = @("441629168735", "451629889107")
```
And it will be transformed to:
```json
[
  {"eway_bill_number": "441629168735"},
  {"eway_bill_number": "451629889107"}
]
```

---

## 📝 Real-World Scenarios

### Scenario 1: Local Delivery (Single Vehicle, Multiple Deliveries)
**Situation:** You have 5 deliveries in Delhi using one truck

```json
{
  "userGstin": "09COVPS5556J1ZT",
  "place_of_consignor": "Delhi Warehouse",
  "state_of_consignor": "Delhi",
  "vehicle_number": "DL01AB1234",
  "mode_of_transport": "1",
  "transporter_document_number": "DLV2025001",
  "transporter_document_date": "07/10/2025",
  "data_source": "erp",
  "list_of_eway_bills": [
    {"eway_bill_number": "441629168735"},
    {"eway_bill_number": "441629168736"},
    {"eway_bill_number": "441629168737"},
    {"eway_bill_number": "441629168738"},
    {"eway_bill_number": "441629168739"}
  ]
}
```

### Scenario 2: Interstate Transport
**Situation:** Moving goods from Delhi to Mumbai via road

```json
{
  "userGstin": "09COVPS5556J1ZT",
  "place_of_consignor": "Delhi",
  "state_of_consignor": "Delhi",
  "vehicle_number": "DL01AB1234",
  "mode_of_transport": "1",
  "transporter_document_number": "IST2025001",
  "transporter_document_date": "07/10/2025",
  "data_source": "erp",
  "list_of_eway_bills": [
    {"eway_bill_number": "441629168735"},
    {"eway_bill_number": "451629889107"}
  ]
}
```

### Scenario 3: Rail Transport (Bulk Shipment)
**Situation:** Sending goods via train with multiple consignments

```json
{
  "userGstin": "09COVPS5556J1ZT",
  "place_of_consignor": "Delhi",
  "state_of_consignor": "Delhi",
  "vehicle_number": "RAIL001",
  "mode_of_transport": "2",
  "transporter_document_number": "RWY2025001",
  "transporter_document_date": "07/10/2025",
  "data_source": "erp",
  "list_of_eway_bills": [
    {"eway_bill_number": "441629168735"},
    {"eway_bill_number": "451629889107"},
    {"eway_bill_number": "461629889108"}
  ]
}
```

---

## 💡 Tips & Best Practices

### ✅ DO's
- ✓ Use valid e-way bill numbers that exist in the system
- ✓ Ensure vehicle number format is correct (e.g., DL01AB1234)
- ✓ Use DD/MM/YYYY format for dates
- ✓ Group e-way bills that are actually being transported together
- ✓ Keep transport document numbers unique and trackable

### ❌ DON'Ts
- ✗ Don't consolidate already consolidated e-way bills
- ✗ Don't use expired e-way bills
- ✗ Don't mix different vehicles in one consolidation
- ✗ Don't leave any required fields empty
- ✗ Don't use incorrect date formats (like MM/DD/YYYY)

---

## 🔍 Validation Checklist

Before creating a consolidated e-way bill, verify:

- [ ] All e-way bills are valid and active
- [ ] Vehicle number is correctly formatted
- [ ] GSTIN is correct
- [ ] Date is in DD/MM/YYYY format
- [ ] Mode of transport matches your vehicle type
- [ ] Transport document number is unique
- [ ] All required fields are filled

---

## 📞 Quick Reference

**Minimum required e-way bills:** 1  
**Maximum e-way bills:** No fixed limit (depends on API)  
**Vehicle number format:** State code + Numbers + Letters + Numbers  
**Date format:** DD/MM/YYYY  
**Time to create:** Usually instant (1-2 seconds)  
**Token validity:** 23 hours (auto-refreshed)

---

## 🎉 Summary

1. **Use test_api.html** for easy testing in Chrome
2. **Fill all required fields** - no shortcuts!
3. **Check your e-way bill numbers** - they must be valid
4. **Use correct date format** - DD/MM/YYYY
5. **See response immediately** - success or error with details

**Happy consolidating! 🚀**
