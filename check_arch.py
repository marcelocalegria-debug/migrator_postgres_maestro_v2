import platform
import struct
import os

print(f"Python version: {platform.python_version()}")
print(f"Python architecture: {platform.architecture()[0]}")
print(f"Pointer size: {struct.calcsize('P') * 8} bits")

dll_path = "fbclient.dll"
if os.path.exists(dll_path):
    print(f"Found {dll_path} in current directory.")
    with open(dll_path, "rb") as f:
        header = f.read(2)
        if header == b"MZ":
            f.seek(60)
            pe_offset = struct.unpack("<I", f.read(4))[0]
            f.seek(pe_offset + 4)
            machine = struct.unpack("<H", f.read(2))[0]
            if machine == 0x014c:
                print(f"{dll_path} architecture: 32-bit (x86)")
            elif machine == 0x8664:
                print(f"{dll_path} architecture: 64-bit (x64)")
            else:
                print(f"{dll_path} architecture: Unknown (Machine code: {hex(machine)})")
        else:
            print(f"{dll_path} is not a valid PE file (no MZ header).")
else:
    print(f"{dll_path} NOT found in current directory.")
