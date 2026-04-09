import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scraper.services import get_workspace_channel_status
from combined_scraper import CONNECTOR_TO_CHANNEL, CHANNEL_LABELS

def test_workspace(workspace_id):
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
                "status": status
            })
            
    channels.sort(key=lambda x: x["name"])
    print(f"{workspace_id}: {channels}")

if __name__ == "__main__":
    test_workspace("lifesight_a8407028") # Dogfooding
    test_workspace("ls_tbc_en_us_aaff3c9e") # PonosUS
    test_workspace("ls_factori_75e76e46") # Factori
    test_workspace("ls_segugio_455d4b1a") # Segugio
