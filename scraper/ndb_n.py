import json
import csv
import os
from google.cloud import datastore
from google.oauth2 import service_account

PROJECT_ID = "moda-platform-prd"

# This script lives in scraper/ — credential_file.json is in the project root (parent dir)
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)

CONFIG_PATH = os.path.join(_PROJECT_ROOT, "credential_file.json")
OUTPUT_PATH = os.path.join(_PROJECT_ROOT, "channel_credentials.csv")

# ---------- auth ----------
with open(CONFIG_PATH) as f:
    cfg = json.load(f)

creds = service_account.Credentials.from_service_account_info(
    cfg["bigquery_credential_file"]
)

ds = datastore.Client(project=PROJECT_ID, credentials=creds)


# ---------- helpers ----------
def _ns(ns):
    return "" if ns is None else str(ns)


def parse_active_details(raw):
    """
    Datastore may store active_ad_account_details as:
    - list[dict]
    - JSON string
    """
    if not raw:
        return []

    if isinstance(raw, list):
        return raw

    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return []

    return []


def get_token_accounts(workspace):
    q = ds.query(kind="tokenactiveaccount", namespace=_ns(workspace))
    return list(q.fetch())


# ---------- extractor ----------
def export_workspace_to_csv(workspace):
    entities = get_token_accounts(workspace)

    if not entities:
        print("No tokenactiveaccount entities found")
        return

    rows = []

    for e in entities:
        connector = e.get("connector")
        token = e.get("token")
        refresh_token = e.get("refresh_token")

        details = parse_active_details(e.get("active_ad_account_details"))

        if not details:
            acc = e.get("account_id")
            login = e.get("login_customer_id")
            cust = e.get("customer_id")

            if acc:
                rows.append([
                    connector,
                    login,
                    cust,
                    acc,
                    refresh_token,
                    token
                ])
            continue

        for d in details:
            rows.append([
                connector,
                d.get("login_customer_id"),
                d.get("customer_id"),
                d.get("account_id"),
                refresh_token,
                token
            ])

    # ---------- write CSV ----------
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "connector",
            "login_customer_id",
            "customer_id",
            "account_id",
            "refresh_token",
            "token"
        ])
        writer.writerows(rows)

    print(f"\nSaved CSV → {OUTPUT_PATH}")
    print(f"Rows written: {len(rows)}")


# ---------- run ----------
if __name__ == "__main__":
    ws = input("Enter workspace namespace: ").strip()
    export_workspace_to_csv(ws)
