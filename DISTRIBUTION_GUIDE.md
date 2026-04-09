# Data Integrity Dashboard — Distribution Guide

This guide explains how to package, distribute, and set up the **Data Integrity Dashboard** locally so that other team members can run it on their own machines.

---

## 📦 What to Distribute

Do **not** send the `node_modules/` or `__pycache__/` folders, and never send your personal `.env` file or `credential_file.json` in plain text over insecure channels.

**Prepare the Project ZIP:**
1. Navigate to `/Users/dipayandhar/Downloads/DataAuditDash/`
2. Select the `data-integrity-dashboard/` folder and compress it into a ZIP file.
3. Ensure these files/folders are **deleted** or ignored before zipping:
   - `node_modules/` (Very large, must be generated on the target machine)
   - `.env` (Contains your personal paths/secrets)
   - `credential_file.json` (GCP Service Account key — must be handled securely)
   - `__pycache__/` (Python cache files)
   - Any large, stale `combined_metrics_*.csv` output files.

---

## 🛠 Prerequisites for the New Machine

Before running the project, the new user must have the following software installed:

1. **Python (3.9 or higher)**
   - Download from: [python.org](https://www.python.org/downloads/)
   - *Windows users: Ensure "Add Python to PATH" is checked during installation.*
2. **Node.js (18 LTS or higher)**
   - Download from: [nodejs.org](https://nodejs.org/en)
   - Includes `npm` which is required for the frontend.
3. **PC-Level VPN (Important for APIs like TikTok)**
   - Ensure you are connected to the corporate VPN if required to bypass geo-restrictions for ad platform APIs. Proxy configurations are deliberately excluded from the application.

---

## 🔐 Credentials Setup 

Because you cannot securely share the `.env` inside the codebase, the new user needs to obtain access via their own local configuration.

1. **The BigQuery & Datastore Key**:
   Securely acquire the `credential_file.json` (Service Account key) from your team lead via 1Password or a secure vault.
   Save this file in the **root** of the extracted `data-integrity-dashboard` folder. It must be named exactly `credential_file.json`.
   
2. **Environment Variables**:
   Duplicate the `.env.example` file and rename it to `.env`.
   Paste any required manual API tokens (e.g. `TIKTOK_ACCESS_TOKEN`) into this `.env` file if they are not dynamically fetched from Datastore.

---

## 🚀 Setup Instructions

Follow these steps exactly on the new machine:

### Step 1: Open the Terminal
Extract the zipped folder. Open the terminal (Mac/Linux) or Command Prompt/PowerShell (Windows), and navigate inside:
```bash
cd path/to/extracted/data-integrity-dashboard
```

### Step 2: Install Python Dependencies
Install the required backend libraries (Flask, Google Cloud SDKs, etc.):
```bash
pip3 install -r requirements.txt
```

### Step 3: Install Node.js Dependencies
Install the required frontend libraries (React, Vite, Tailwind):
```bash
npm install
```

### Step 4: Verify Credentials
Ensure `credential_file.json` and `.env` are present precisely in the `data-integrity-dashboard` main folder alongside `app.py`.

---

## ▶️ Running the Dashboard

You need to keep two terminal windows open simultaneously (one for the backend, one for the frontend).

### Terminal 1: Start the Backend (Flask API)
```bash
python3 app.py
```
*(Leave this window open. It should display "Running on http://0.0.0.0:3000")*

### Terminal 2: Start the Frontend (Vite)
```bash
npm run dev
```
*(Leave this open. It provides the local link, usually `http://localhost:5173`)*

Open your browser and navigate to: **http://localhost:5173**

---

## ⚠️ Troubleshooting

| Issue | Cause | Fix |
|---|---|---|
| `python3: command not found` | Python isn't installed or not in PATH | Install Python and check "Add to PATH" |
| `npm: command not found` | Node.js isn't installed | Install Node LTS |
| **API error 500 when running Scraper** | Missing Google Credentials | Ensure `credential_file.json` exactly matches the name and path |
| **TikTok/Amazon Scraper Fails** | Missing VPN connection or local tokens | Connect to the PC-Level VPN. Check `.env`. |
| **CORS Errors in browser console** | Flask backend isn't running | Start `app.py` in Terminal 1 |
