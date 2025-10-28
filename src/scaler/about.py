import os

version_file_path = os.path.join(os.path.dirname(__file__), "version.txt")
with open(version_file_path, "r") as f:
    __version__ = f.read().strip()
