import argparse
import logging
import csv
import os
from datetime import datetime

from services import get_bq_client, fetch_channel_credentials
from connectors.tiktok import fetch_tiktok_data
from connectors.amazon import fetch_amazon_data
from connectors.pinterest import fetch_pinterest_data

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Unified Ads Scraper")
    parser.add_argument("--workspace", required=True, help="Workspace ID(s) (comma-separated) or 'all'")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--channels", default="all", help="Comma-separated list of channels (tiktok,amazon,pinterest)")
    
    # Optional overrides
    parser.add_argument("--amazon-client-id", help="Amazon Client ID")
    parser.add_argument("--amazon-client-secret", help="Amazon Client Secret")

    args = parser.parse_args()
    
    workspaces_input = args.workspace
    start_date = args.start_date
    end_date = args.end_date
    
    # Determine workspaces
    if workspaces_input.lower() == "all":
        # TODO: Implement fetching all workspaces if 'all' is passed
        # For now, we expect a list or a single ID
        logger.warning("'all' workspaces not yet fully implemented in CLI args, please provide comma-separated list")
        workspace_ids = [] 
    else:
        workspace_ids = [ws.strip() for ws in workspaces_input.split(",") if ws.strip()]

    channels = args.channels.split(",") if args.channels != "all" else ["tiktok", "amazon", "pinterest"]
    
    all_data = []

    for workspace_id in workspace_ids:
        logger.info(f"Processing workspace: {workspace_id}")
        
        # --- 1. Fetch API Data ---
        
        if "tiktok" in channels:
            creds = fetch_channel_credentials(workspace_id, "tiktok")
            if creds:
                data = fetch_tiktok_data(creds, start_date, end_date)
                # Add workspace_name/id to rows
                for row in data:
                    row["workspace_name"] = workspace_id
                all_data.extend(data)
            else:
                logger.warning(f"No TikTok credentials found for {workspace_id}.")

        if "amazon" in channels:
            creds = fetch_channel_credentials(workspace_id, "amazon")
            if creds:
                # Prefer args, fallback to env
                client_id = args.amazon_client_id or os.getenv("AMAZON_CLIENT_ID")
                client_secret = args.amazon_client_secret or os.getenv("AMAZON_CLIENT_SECRET")
                
                data = fetch_amazon_data(creds, start_date, end_date, client_id, client_secret)
                for row in data:
                    row["workspace_name"] = workspace_id
                all_data.extend(data)
            else:
                logger.warning(f"No Amazon credentials found for {workspace_id}.")

        if "pinterest" in channels:
            creds = fetch_channel_credentials(workspace_id, "pinterest")
            if creds:
                data = fetch_pinterest_data(creds, start_date, end_date)
                for row in data:
                    row["workspace_name"] = workspace_id
                all_data.extend(data)
            else:
                logger.warning(f"No Pinterest credentials found for {workspace_id}.")

    # --- 2. Fetch BigQuery Data (for comparison) ---
    # (Simplified for this example - fetching all relevant rows)
    
    # bq_client = get_bq_client()
    
    # Construct query to get BQ data for the same period and channels
    # Note: This assumes the table exists and follows the schema
    
    # ... (BigQuery fetching logic would go here, similar to original script but optimized) ...
    
    # --- 3. Output CSV ---
    if all_data:
        # Create a combined filename
        ws_label = workspace_ids[0] if len(workspace_ids) == 1 else "MULTI"
        filename = f"combined_data_{ws_label}_{start_date}_{end_date}.csv"
        # Ensure absolute path for clarity or relative to cwd
        filepath = os.path.abspath(filename)
        
        keys = ["date", "workspace_name", "source", "ad_account_id", "api_impressions", "api_clicks", "api_spends", "bigquery_impressions", "bigquery_clicks", "bigquery_spends"]
        
        # Normalize data for CSV
        csv_rows = []
        for row in all_data:
            csv_rows.append({
                "date": row.get("date"),
                "workspace_name": row.get("workspace_name", ""),
                "source": row.get("source"),
                "ad_account_id": row.get("ad_account_id"),
                "api_impressions": row.get("impressions", 0),
                "api_clicks": row.get("clicks", 0),
                "api_spends": row.get("spend", 0.0),
                "bigquery_impressions": 0, # Placeholder
                "bigquery_clicks": 0,      # Placeholder
                "bigquery_spends": 0.0     # Placeholder
            })
            
        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(csv_rows)
            
        logger.info(f"Data saved to {filepath}")
        # Print filename to stdout for the Node server to capture
        print(f"OUTPUT_FILE:{filepath}")
    else:
        logger.info("No data fetched.")

if __name__ == "__main__":
    main()
