import google
import os

try:
    import google.adk
    print(f"google.adk found at: {google.adk.__file__}")
    print(f"google.adk dir: {dir(google.adk)}")
except ImportError as e:
    print(f"Could not import google.adk: {e}")

try:
    import adk
    print(f"adk found at: {adk.__file__}")
except ImportError:
    print("adk not found")

print(f"google package path: {google.__path__}")
