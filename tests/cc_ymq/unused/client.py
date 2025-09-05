import socket

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("192.0.2.2", 2323))
print(f"[*] Connected to 192.0.2.2:2323")
s.sendall(b"Hello, World!\n")
print("[*] Sent message")

msg = s.recv(4096)
print(f"[*] Received message: {msg}")
