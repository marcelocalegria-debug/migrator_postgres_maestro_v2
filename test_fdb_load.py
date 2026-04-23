import fdb
import os
import sys

dll_path = os.path.abspath("fbclient.dll")
print(f"Attempting to load: {dll_path}")
if not os.path.exists(dll_path):
    print("DLL does not exist at this path.")
    sys.exit(1)

try:
    fdb.load_api(dll_path)
    print("Successfully loaded fdb API with explicitly provided path.")
except Exception as e:
    print(f"Failed to load fdb API: {e}")
    import traceback
    traceback.print_exc()
