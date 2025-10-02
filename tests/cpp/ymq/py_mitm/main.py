# flake8: noqa: E402

"""
This script provides a framework for running MITM test cases
"""

import argparse
import os
import signal
import subprocess
import types
from typing import List
from scapy.all import IP, TCP, TunTapInterface  # type: ignore

from tests.cpp.ymq.py_mitm.types import AbstractMITM, TCPConnection
from tests.cpp.ymq.py_mitm import passthrough, randomly_drop_packets, send_rst_to_client


def echo_call(cmd: List[str]):
    print(f"+ {' '.join(cmd)}")
    subprocess.check_call(cmd)


def create_tuntap_interface(iface_name: str, mitm_ip: str, remote_ip: str) -> TunTapInterface:
    """
    Creates a TUNTAP interface and sets brings it up and adds ips using the `ip` program

    Args:
        iface_name: The name of the TUNTAP interface, usually like `tun0`, `tun1`, etc.
        mitm_ip: The desired ip address of the mitm. This is the ip that clients can use to connect to the mitm
        remote_ip: The ip that routes to/from the tuntap interface.
        packets sent to `mitm_ip` will appear to come from `remote_ip`,\
        and conversely the tuntap interface can connect/send packets
        to `remote_ip`, making it a suitable ip for binding a server

    Returns:
        The TUNTAP interface
    """
    iface = TunTapInterface(iface_name, mode="tun")

    try:
        echo_call(["sudo", "ip", "link", "set", iface_name, "up"])
        echo_call(["sudo", "ip", "addr", "add", remote_ip, "peer", mitm_ip, "dev", iface_name])
        print(f"[+] Interface {iface_name} up with IP {mitm_ip}")
    except subprocess.CalledProcessError:
        print("[!] Could not bring up interface. Run as root or set manually.")
        raise

    return iface


def main(pid: int, mitm_ip: str, mitm_port: int, remote_ip: str, server_port: int, mitm: AbstractMITM):
    """
    This function serves as a framework for man in the middle implementations
    A client connects to the MITM, then the MITM connects to a remote server
    The MITM sits inbetween the client and the server, manipulating the packets sent depending on the test case
    This function:
        1. creates a TUNTAP interface and prepares it for MITM
        2. handles connecting clients and handling connection closes
        3. delegates additional logic to a pluggable callable, `mitm`
        4. returns when both connections have terminated (via )

    Args:
        pid: this is the pid of the test process, used for signaling readiness \
        we send SIGUSR1 to this process when the mitm is ready
        mitm_ip: The desired ip address of the mitm server
        mitm_port: The desired port of the mitm server. \
        This is the port used to connect to the server, but the client is free to connect on any port
        remote_ip: The desired remote ip for the TUNTAP interface. This is the only ip address \
        reachable by the interface and is thus the src ip for clients, and the ip that the remote server \
        must be bound to
        server_port: The port that the remote server is bound to
        mitm: The core logic for a MITM test case. This callable may maintain its own state and is responsible \
        for sending packets over the TUNTAP interface (if it doesn't, nothing will happen)
    """

    tuntap = create_tuntap_interface("tun0", mitm_ip, remote_ip)

    # signal the caller that the tuntap interface has been created
    if pid > 0:
        os.kill(pid, signal.SIGUSR1)

    # these track information about our connections
    # we already know what to expect for the server connection, we are the connector
    client_conn = None

    # the port that the mitm uses to connect to the server
    # we increment the port for each new connection to avoid collisions
    mitm_server_port = mitm_port
    server_conn = TCPConnection(mitm_ip, mitm_server_port, remote_ip, server_port)

    # tracks the state of each connection
    client_sent_fin_ack = False
    client_closed = False
    server_sent_fin_ack = False
    server_closed = False

    while True:
        pkt = tuntap.recv()
        if not pkt.haslayer(IP) or not pkt.haslayer(TCP):
            continue
        ip = pkt[IP]
        tcp = pkt[TCP]

        # for a received packet, the destination ip and port are our local ip and port
        # and the source ip and port will be the remote ip and port
        sender = TCPConnection(pkt.dst, pkt.dport, pkt.src, pkt.sport)

        pretty = f"[{tcp.flags}]{(': ' + str(bytes(tcp.payload))) if tcp.payload else ''}"

        if not mitm.proxy(tuntap, pkt, sender, client_conn, server_conn):
            if sender == client_conn:
                print(f"[DROPPED]: -> {pretty}")
            elif sender == server_conn:
                print(f"[DROPPED]: <- {pretty}")
            else:
                print(f"[DROPPED]: ?? {pretty}")

            continue  # the segment was not proxied, so we can't update our internal state

        if sender == client_conn:
            print(f"-> {pretty}")
        elif sender == server_conn:
            print(f"<- {pretty}")

        if tcp.flags == "S":  # SYN from client
            print("-> [S]")
            if sender != client_conn or client_conn is None:
                print(f"[*] New connection from {ip.src}:{tcp.sport} to {ip.dst}:{tcp.dport}")
                client_conn = sender

                server_conn = TCPConnection(mitm_ip, mitm_server_port, remote_ip, server_port)

                # increment the port so that the next client connection (if there is one) uses a different port
                mitm_server_port += 1

        if tcp.flags == "SA":  # SYN-ACK from server
            if sender == server_conn:
                print(f"[*] Connection to server established: {ip.src}:{tcp.sport} to {ip.dst}:{tcp.dport}")

        if tcp.flags.F and tcp.flags.A:  # FIN-ACK
            if sender == client_conn:
                client_sent_fin_ack = True
            if sender == server_conn:
                server_sent_fin_ack = True

        if tcp.flags.A:  # ACK
            if sender == client_conn and server_sent_fin_ack:
                server_closed = True
            if sender == server_conn and client_sent_fin_ack:
                client_closed = True

        if client_closed and server_closed:
            print("[*] Both connections closed")
            return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Man in the middle test framework")
    parser.add_argument("pid", type=int, help="The pid of the test process, used for signaling")
    parser.add_argument("testcase", type=str, help="The MITM test case module name")
    parser.add_argument("mitm_ip", type=str, help="The desired ip address of the mitm server")
    parser.add_argument("mitm_port", type=int, help="The desired port of the mitm server")
    parser.add_argument("remote_ip", type=str, help="The desired remote ip for the TUNTAP interface")
    parser.add_argument("server_port", type=int, help="The port that the remote server is bound to")

    args, unknown = parser.parse_known_args()

    # TODO: use `match` in Python 3.10+
    module: types.ModuleType
    if args.testcase == "passthrough":
        module = passthrough
    elif args.testcase == "randomly_drop_packets":
        module = randomly_drop_packets
    elif args.testcase == "send_rst_to_client":
        module = send_rst_to_client
    else:
        raise ValueError(f"Unknown testcase: {args.testcase}")

    main(args.pid, args.mitm_ip, args.mitm_port, args.remote_ip, args.server_port, module.MITM(*unknown))
