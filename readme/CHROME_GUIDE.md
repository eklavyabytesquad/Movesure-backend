# 🌐 Chrome Browser Guide - E-Way Bill API

## 🚀 Quick Start (3 Steps!)

### Step 1: Make sure your Flask server is running
```bash
python App.py
```
You should see: `Server running at: http://localhost:5000`

### Step 2: Open the test page in Chrome
- Navigate to: `c:\Desktop\SS TRANSPORT\Backend\test_api.html`
- **Double-click** the file to open in Chrome
- Or **drag and drop** into Chrome browser

### Step 3: Start testing!
The page has a beautiful interface to test all your APIs!

---

## 📋 What You Can Do in Chrome

### ✅ 1. Test Server Health
- Click "✅ Test Health" button
- Confirms server is running
- Shows current timestamp

### 🔍 2. Get E-Way Bill Details (GET Request)
**What it does:** Retrieves details of a specific e-way bill

**How to use:**
1. Enter E-Way Bill Number (e.g., `441629168735`)
2. Enter GSTIN (e.g., `09COVPS5556J1ZT`)
3. Click "🔍 Get E-Way Bill Details"
4. See the response below with all details

**Example Response:**
```json
{
  "status": "success",
  "message": "E-Way Bill details retrieved successfully",
  "data": {
    "ewayBillNumber": "441629168735",
    "documentNumber": "DOC123",
    "fromGstin": "09COVPS5556J1ZT",
    "toGstin": "27AAAAA0000A1Z5",
    "transporterName": "ABC Transport",
    "vehicleNumber": "DL01AB1234"
    // ... more details
  }
}
```

### 📦 3. Create Consolidated E-Way Bill (POST Request)
**What it does:** Combines multiple e-way bills for a single vehicle/shipment

**How to use:**
1. Fill in all the form fields:
   - **User GSTIN:** Your company's GSTIN
   - **Place of Consignor:** Starting point (e.g., "Delhi")
   - **State of Consignor:** State name (e.g., "Delhi")
   - **Vehicle Number:** Registration number (e.g., "DL01AB1234")
   - **Mode of Transport:** Select from dropdown (Road/Rail/Air/Ship)
   - **Transporter Document Number:** Your document number
   - **Transporter Document Date:** Date in DD/MM/YYYY format
   - **Data Source:** ERP or Web
   - **List of E-Way Bills:** Comma-separated numbers

2. Click "🚀 Create Consolidated E-Way Bill"
3. See the consolidated e-way bill response

**Example Input:**
```
User GSTIN: 09COVPS5556J1ZT
Place: Delhi
State: Delhi
Vehicle: DL01AB1234
Mode: 1 (Road)
Doc Number: DOC123456
Date: 07/10/2025
Data Source: erp
E-Way Bills: 441629168735, 451629889107, 451629889108
```

**Example Response:**
```json
{
  "status": "success",
  "message": "Consolidated E-Way Bill created successfully",
  "data": {
    "consolidatedEwayBillNumber": "CEW123456789",
    "vehicleNumber": "DL01AB1234",
    "totalEwayBills": 3,
    "ewayBills": ["441629168735", "451629889107", "451629889108"],
    "validUpto": "2025-10-08T23:59:59"
  }
}
```

### 🔄 4. Refresh JWT Token
- Click "🔄 Refresh Token" button
- Gets a fresh JWT token from the server
- Updates `jwt_token.json` file automatically

---

## 🎨 Features of the Test Page

### ✨ Beautiful UI
- Modern gradient design
- Easy-to-read forms
- Color-coded responses (Green = Success, Red = Error)

### 📊 JSON Response Viewer
- Pretty-printed JSON
- Easy to read and understand
- Syntax highlighted

### ⚡ Real-time Testing
- Instant API calls
- Loading indicators
- Smooth animations
- Auto-scroll to responses

### 🔍 Response Types
- **Success (Green):** ✅ Success Response
- **Error (Red):** ❌ Error Response
- Shows complete JSON data

---

## 💡 Common Use Cases

### Use Case 1: Check Single E-Way Bill Status
```
1. Open test_api.html in Chrome
2. Enter e-way bill number: 441629168735
3. Enter GSTIN: 09COVPS5556J1ZT
4. Click "Get E-Way Bill Details"
5. View complete e-way bill information
```

### Use Case 2: Create Consolidated E-Way Bill for Multiple Shipments
```
1. Open test_api.html in Chrome
2. Fill in vehicle and transport details
3. Enter multiple e-way bill numbers (comma-separated):
   441629168735, 451629889107, 451629889108
4. Click "Create Consolidated E-Way Bill"
5. Get consolidated e-way bill number
```

### Use Case 3: Verify Server is Running
```
1. Open test_api.html in Chrome
2. Click "Test Health" button
3. See server status and timestamp
```

---

## 🔗 Direct Chrome URLs (Alternative Method)

### For GET Requests Only:
You can also paste these URLs directly in Chrome's address bar:

**Health Check:**
```
http://localhost:5000/api/health
```

**Get E-Way Bill:**
```
http://localhost:5000/api/ewaybill?eway_bill_number=441629168735&gstin=09COVPS5556J1ZT
```

**Note:** For POST requests (Consolidated E-Way Bill), you MUST use the HTML test page or Postman.

---

## 🛠️ Troubleshooting

### Problem: "Failed to fetch" error
**Solution:** Make sure Flask server is running (`python App.py`)

### Problem: "Network error"
**Solution:** Check if you're using `http://localhost:5000` (not https)

### Problem: CORS error
**Solution:** Already handled! CORS is enabled in the Flask app

### Problem: Token expired
**Solution:** Click "🔄 Refresh Token" button in the test page

### Problem: Page not loading
**Solution:** Make sure you're opening `test_api.html` from the correct folder:
`c:\Desktop\SS TRANSPORT\Backend\test_api.html`

---

## 📸 Visual Guide

### The Test Page Looks Like This:

```
╔═══════════════════════════════════════╗
║   🚛 E-Way Bill API Tester           ║
║   Test your Flask API endpoints      ║
╠═══════════════════════════════════════╣
║                                       ║
║  📋 1. Get E-Way Bill Details        ║
║  ┌─────────────────────────────────┐ ║
║  │ E-Way Bill Number: [________]   │ ║
║  │ GSTIN: [___________________]    │ ║
║  │ [🔍 Get E-Way Bill Details]     │ ║
║  └─────────────────────────────────┘ ║
║                                       ║
║  📦 2. Create Consolidated E-Way Bill║
║  ┌─────────────────────────────────┐ ║
║  │ User GSTIN: [______________]    │ ║
║  │ Place: [_________________]      │ ║
║  │ Vehicle: [_______________]      │ ║
║  │ E-Way Bills: [__________]       │ ║
║  │ [🚀 Create Consolidated]        │ ║
║  └─────────────────────────────────┘ ║
║                                       ║
║  Response:                            ║
║  ✅ Success Response                  ║
║  { "status": "success", ... }         ║
╚═══════════════════════════════════════╝
```

---

## 🎯 Pro Tips

1. **Keep the test page open** - No need to close and reopen
2. **Pre-filled values** - Default values are already filled for quick testing
3. **Token auto-refresh** - Token automatically refreshes when expired
4. **Response saved** - All responses are saved to JSON files in the Backend folder
5. **Multiple e-way bills** - Use commas to separate multiple e-way bill numbers

---

## 📞 Need Help?

1. Check the Flask terminal for detailed logs
2. Look for error messages in Chrome's Console (F12)
3. Verify all form fields are filled correctly
4. Make sure e-way bill numbers are valid

---

## 🎉 Summary

**Easiest Way to Test:**
1. Run: `python App.py`
2. Open: `test_api.html` in Chrome
3. Click buttons and see results!

**That's it! No Postman needed, no command-line required - just Chrome! 🚀**
