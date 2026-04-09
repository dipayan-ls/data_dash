"""
Data Integrity Dashboard — Flask Backend

Serves the Vite-built React frontend and exposes API routes for the scraper.
Imports from the modular services/ package instead of the old monolith.
"""

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import os
import sys
import traceback

# Ensure imports resolve from this directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from services.orchestrator import run_scraper_api, load_workspace_metadata
from services.registry import CHANNEL_LABELS, CONNECTOR_TO_CHANNEL
from services.credentials import get_workspace_channel_status

app = Flask(__name__, static_folder=os.path.join(BASE_DIR, "dist"), static_url_path="")
CORS(app)


# ---------------------------------------------------------------------------
# API Routes
# ---------------------------------------------------------------------------

@app.route("/api/workspaces")
def get_workspaces():
    """Return all workspaces from Workspace_information.csv."""
    try:
        workspaces = load_workspace_metadata()
        return jsonify([{"id": ws[0], "name": ws[1]} for ws in workspaces])
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/channels")
def get_channels():
    """Return all supported ad platform channels."""
    return jsonify([{"id": k, "name": v} for k, v in CHANNEL_LABELS.items()])


@app.route("/api/workspace-channels")
def get_workspace_channels():
    """Return channels configured for a workspace with their status."""
    workspace_id = request.args.get("workspace_id")
    if not workspace_id:
        return jsonify({"error": "Missing workspace_id parameter"}), 400

    try:
        status_map = get_workspace_channel_status(workspace_id)
        channels = []
        seen = set()
        for connector, status in status_map.items():
            channel_key = CONNECTOR_TO_CHANNEL.get(connector.lower())
            if channel_key and channel_key not in seen:
                seen.add(channel_key)
                channels.append({
                    "id": channel_key,
                    "name": CHANNEL_LABELS.get(channel_key, channel_key),
                    "status": status,
                })
        channels.sort(key=lambda x: x["name"])
        return jsonify(channels)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/scrape", methods=["POST"])
def scrape():
    """
    Trigger the scraper and return the resulting CSV as a string.

    Body (JSON):
        workspace_ids: list[str] | "all"
        start_date:    str  (YYYY-MM-DD)
        end_date:      str  (YYYY-MM-DD)
        channels:      list[str]
        granularity:   str  (daily|monthly|yearly|overall)
    """
    data = request.json or {}
    workspace_ids = data.get("workspace_ids", [])
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    channels = data.get("channels", [])
    granularity = data.get("granularity", "daily")

    if not start_date or not end_date or not channels:
        return jsonify({"error": "Missing parameters: start_date, end_date, channels are required"}), 400

    # Validate date format
    import datetime
    for label, val in [("start_date", start_date), ("end_date", end_date)]:
        try:
            datetime.datetime.strptime(val, "%Y-%m-%d")
        except ValueError:
            return jsonify({"error": f"Invalid {label} format. Expected YYYY-MM-DD."}), 400

    if start_date > end_date:
        return jsonify({"error": "start_date must be before or equal to end_date."}), 400

    # Validate channels
    invalid = [ch for ch in channels if ch not in CHANNEL_LABELS]
    if invalid:
        return jsonify({"error": f"Unknown channels: {', '.join(invalid)}"}), 400

    try:
        all_workspaces = load_workspace_metadata()
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": f"Failed to load workspaces: {e}"}), 500

    if not workspace_ids or workspace_ids == "all":
        selected_workspaces = all_workspaces
    else:
        selected_workspaces = [ws for ws in all_workspaces if ws[0] in workspace_ids]

    if not selected_workspaces:
        return jsonify({"error": "No matching workspaces found"}), 400

    try:
        csv_content = run_scraper_api(selected_workspaces, start_date, end_date, channels, granularity)
        ws_label = selected_workspaces[0][1] if len(selected_workspaces) == 1 else "MULTI"
        filename = f"combined_metrics_{ws_label}_{start_date}_to_{end_date}.csv"
        return jsonify({"csv": csv_content, "filename": filename})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Frontend — serve Vite build in production
# ---------------------------------------------------------------------------

@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve_frontend(path):
    dist_dir = os.path.join(BASE_DIR, "dist")
    if path and os.path.exists(os.path.join(dist_dir, path)):
        return send_from_directory(dist_dir, path)
    index = os.path.join(dist_dir, "index.html")
    if os.path.exists(index):
        return send_from_directory(dist_dir, "index.html")
    return (
        "Frontend not built. Run 'npm install && npm run build', "
        "or start Vite separately with 'npm run dev'.",
        200,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3003))
    print(f"Starting Data Integrity Dashboard on http://0.0.0.0:{port}")
    print("  API:      /api/workspaces  /api/channels  /api/scrape")
    print("  Frontend: http://localhost:5173  (run 'npm run dev')")
    app.run(host="0.0.0.0", port=port, debug=False)
