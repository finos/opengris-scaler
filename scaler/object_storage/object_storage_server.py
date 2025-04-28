import ctypes
import sys


def load_library(lib_path):
    """
    Load the shared dynamic library.
    """
    try:
        return ctypes.CDLL(lib_path)
    except OSError as e:
        print(f"Failed to load library: {e}")
        sys.exit(1)


def run_object_storage_server(lib_path: str, name: str = "127.0.0.1", port: str = "55555"):
    # Load the library
    lib = load_library(lib_path=lib_path)
    # Define the argument and return types for the main_entrance function
    lib.run_object_storage_server.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
    lib.run_object_storage_server.restype = None

    # Call the function
    name_bytes = name.encode("utf-8")
    port_bytes = port.encode("utf-8")

    lib.run_object_storage_server(name_bytes, port_bytes)
