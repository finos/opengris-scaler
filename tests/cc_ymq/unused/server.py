import socket

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("192.0.2.1", 12372))
s.listen(5)

p = s.accept()[0]
print("[*] Accepted connection")
msg = p.recv(4096)
print(f"[*] Received message: {msg}")

p.sendall(b"Hello, Client!\n")
