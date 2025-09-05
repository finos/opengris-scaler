#!/usr/bin/env python3
import subprocess
import dataclasses
from scapy.all import TunTapInterface, IP, TCP

SERVER_IP = "192.0.2.1"
SERVER_PORT = 12372
MITM_IP = "192.0.2.2"
MITM_PORT = 2323

@dataclasses.dataclass
class TCPConnection:
    local_ip: str
    local_port: int
    remote_ip: str
    remote_port: int

    def rewrite(self, pkt, ack: int | None = None, data=None):
        ip = pkt[IP]
        tcp = pkt[TCP]

        return IP(src=self.local_ip, dst=self.remote_ip) / TCP(sport=self.local_port, dport=self.remote_port, flags=tcp.flags, seq=tcp.seq, ack=ack or tcp.ack) / bytes(data or tcp.payload)

def create_tun_interface(iface_name="tun0"):
    iface = TunTapInterface(iface_name, mode="tun")

    try:
        subprocess.check_call(["ip", "link", "set", iface_name, "up"])
        subprocess.check_call(["ip", "addr", "add", SERVER_IP, "peer", MITM_IP, "dev", iface_name])
        print(f"[+] Interface {iface_name} up with IP {MITM_IP}")
    except subprocess.CalledProcessError:
        print("[!] Could not bring up interface. Run as root or set manually.")
        raise

    return iface


def main():
    tuntap = create_tun_interface("tun0")

    client_conn = None
    server_conn = TCPConnection(MITM_IP, MITM_PORT, SERVER_IP, SERVER_PORT)

    while True:
        pkt = tuntap.recv()
        if not pkt.haslayer(TCP):
            continue

        ip = pkt[IP]
        tcp = pkt[TCP]

        sender = TCPConnection(ip.dst, tcp.dport, ip.src, tcp.sport)

        if sender == client_conn:
            print(f"-> [{tcp.flags}]{(': ' + str(bytes(tcp.payload))) if tcp.payload else ''}")
        elif sender == server_conn:
            print(f"<- [{tcp.flags}]{(': ' + str(bytes(tcp.payload))) if tcp.payload else ''}")

        if tcp.flags == "S":  # SYN from client
            print("-> [S]")
            print(f"[*] New connection from {ip.src}:{tcp.sport} to {ip.dst}:{tcp.dport}")
            client_conn = sender
        
        if tcp.flags == "SA":  # SYN-ACK from server
            if sender == server_conn:
                print(f"[*] Connection to server established: {ip.src}:{tcp.sport} to {ip.dst}:{tcp.dport}")

        if sender == client_conn:
            tuntap.send(server_conn.rewrite(pkt))
        elif sender == server_conn:
            tuntap.send(client_conn.rewrite(pkt))


if __name__ == "__main__":
    main()
