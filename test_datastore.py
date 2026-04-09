import sys
import os

# Ensure we import from the local source tree
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from scraper.services import get_workspace_channel_status
    
    # Test for Dogfooding and PonosUS
    print("Testing Dogfooding:", get_workspace_channel_status("lifesight_a8407028"))
    print("Testing PonosUS:", get_workspace_channel_status("ls_tbc_en_us_aaff3c9e"))
    print("Testing Factori:", get_workspace_channel_status("ls_factori_75e76e46"))
    print("Testing Segugio:", get_workspace_channel_status("ls_segugio_455d4b1a"))

except Exception as e:
    import traceback
    traceback.print_exc()
