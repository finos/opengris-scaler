#!/usr/bin/env python3
import subprocess
import dataclasses
import sys
from scapy.all import TunTapInterface, IP, TCP


@dataclasses.dataclass
class TCPConnection:
    local_ip: str
    local_port: int
    remote_ip: str
    remote_port: int

    def rewrite(self, pkt, ack: int | None = None, data=None):
        ip = pkt[IP]
        tcp = pkt[TCP]

        return (
            IP(src=self.local_ip, dst=self.remote_ip)
            / TCP(sport=self.local_port, dport=self.remote_port, flags=tcp.flags, seq=tcp.seq, ack=ack or tcp.ack)
            / bytes(data or tcp.payload)
        )


def create_tun_interface(iface_name: str, mitm_ip: str, server_ip: str):
    iface = TunTapInterface(iface_name, mode="tun")

    try:
        subprocess.check_call(["sudo", "ip", "link", "set", iface_name, "up"])
        subprocess.check_call(["sudo", "ip", "addr", "add", server_ip, "peer", mitm_ip, "dev", iface_name])
        print(f"[+] Interface {iface_name} up with IP {mitm_ip}")
    except subprocess.CalledProcessError:
        print("[!] Could not bring up interface. Run as root or set manually.")
        raise

    return iface


def main(mitm_ip: str, mitm_port: int, server_ip: str, server_port: int):
    tuntap = create_tun_interface("tun0", mitm_ip, server_ip)

    client_conn = None
    server_conn = TCPConnection(mitm_ip, mitm_port, server_ip, server_port)

    last_client_pkt = None
    last_server_pkt = None

    client_sent_fin_ack = False
    client_closed = False
    server_sent_fin_ack = False
    server_closed = False

    client_pshack_counter = 0
    server_pshack_counter = 0

    while True:
        pkt = tuntap.recv()
        if not pkt.haslayer(TCP):
            continue
        ip = pkt[IP]
        tcp = pkt[TCP]

        sender = TCPConnection(ip.dst, tcp.dport, ip.src, tcp.sport)

        payload_pretty = (": " + str(bytes(tcp.payload))) if tcp.payload else ""
        if sender == client_conn:
            print(f"-> [{tcp.flags}]{payload_pretty}")
        elif sender == server_conn:
            print(f"<- [{tcp.flags}]{payload_pretty}")
        elif tcp.flags != "S":
            print(f"??? [{tcp.flags}] from unknown sender {ip.src}:{tcp.sport} to {ip.dst}:{tcp.dport}")

        if tcp.flags == "S":  # SYN from client
            print("-> [S]")
            print(f"[*] New connection from {ip.src}:{tcp.sport} to {ip.dst}:{tcp.dport}")
            client_conn = sender

        if tcp.flags == "SA":  # SYN-ACK from server
            if sender == server_conn:
                print(f"[*] Connection to server established: {ip.dst}:{tcp.dport} to {ip.src}:{tcp.sport}")

        if tcp.flags == "FA":  # FIN-ACK
            if sender == client_conn:
                client_sent_fin_ack = True
            if sender == server_conn:
                server_sent_fin_ack = True

        if tcp.flags == "A":  # ACK
            if sender == client_conn and server_sent_fin_ack:
                server_closed = True
            if sender == server_conn and client_sent_fin_ack:
                client_closed = True

        if tcp.flags == "PA":  # PSH-ACK
            if sender == client_conn:
                client_pshack_counter += 1
                if client_pshack_counter == 2:
                    # send an rst to the client to simulate a dropped connection
                    print("^^^ not sent!")
                    npkt = IP(src=client_conn.local_ip, dst=client_conn.remote_ip) / TCP(sport=client_conn.local_port, dport=client_conn.remote_port, flags="FR", seq=tcp.ack)
                    print(f"<- [{npkt[TCP].flags}] (simulated) !!!")
                    tuntap.send(npkt)
                    continue
            if sender == server_conn:
                server_pshack_counter += 1
                if server_pshack_counter == 3:
                    pass

        if sender == client_conn:
            tuntap.send(server_conn.rewrite(pkt))
            last_client_pkt = pkt
        elif sender == server_conn and client_conn is not None:
            tuntap.send(client_conn.rewrite(pkt))
            last_server_pkt = pkt

        if client_closed and server_closed:
            print("[*] Both connections closed")
            return


if __name__ == "__main__":
    mitm_ip, mitm_port, server_ip, server_port = sys.argv
    main(mitm_ip, int(mitm_port), server_ip, int(server_port))
