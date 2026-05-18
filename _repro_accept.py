"""Controlled reproduction: does a YMQ ConnectorSocket fail to connect to a
listening, idle/busy BinderSocket when many connectors connect at once?

This isolates the exact primitive from the CI failure: scheduler/OSS = a
single-IO-thread YMQ BinderSocket; processors/client-agent = separate processes
each doing ConnectorSocket.connect (8-retry give-up). We do NOT touch retry
limits. We only measure success/failure under concurrency, with optional
existing connections generating IO load on the binder's event loop (the
"event loop busy with IO starves accept" hypothesis).

Usage: python _repro_accept.py <burst_N> <warm_K> <io_threads> <busy 0|1>
"""
import multiprocessing as mp
import sys
import time

CTX = mp.get_context("spawn")


def _binder_proc(io_threads: int, addr_q, stop_ev, ready_ev):
    from scaler.io.ymq import BinderSocket, IOContext
    from scaler.utility.logging.utility import setup_logger

    setup_logger()
    ctx = IOContext(num_threads=io_threads)
    binder = BinderSocket(ctx, "binder")
    addr = binder.bind_to_sync("tcp://127.0.0.1:0")
    addr_q.put(repr(addr))
    ready_ev.set()
    # Live server: keep draining whatever arrives so the binder behaves like a
    # real scheduler/OSS (accept + recv on its IO thread).
    while not stop_ev.is_set():
        try:
            binder.recv_message_sync(timeout=0.2)
        except Exception:
            pass


def _warm_proc(addr: str, idx: int, busy: bool, stop_ev, up_q):
    from scaler.io.ymq import Bytes, ConnectorSocket, IOContext
    from scaler.utility.logging.utility import setup_logger

    setup_logger()
    ctx = IOContext(num_threads=1)
    try:
        sock = ConnectorSocket.connect(ctx, f"warm-{idx}", addr)
    except BaseException as e:  # noqa
        up_q.put(("warm-fail", idx, type(e).__name__))
        return
    up_q.put(("warm-ok", idx, ""))
    while not stop_ev.is_set():
        if busy:
            try:
                sock.send_message_sync(Bytes(b"x" * 64), timeout=1.0)
            except Exception:
                pass
        else:
            time.sleep(0.2)


def _burst_proc(addr: str, idx: int, barrier, res_q):
    from scaler.io.ymq import Bytes, ConnectorSocket, IOContext
    from scaler.utility.logging.utility import setup_logger

    setup_logger()
    ctx = IOContext(num_threads=1)
    barrier.wait()  # all burst connectors fire ConnectorSocket.connect together
    t0 = time.time()
    try:
        sock = ConnectorSocket.connect(ctx, f"burst-{idx}", addr)
        sock.send_message_sync(Bytes(b"hello"), timeout=5.0)
        res_q.put(("ok", idx, round(time.time() - t0, 1), ""))
    except BaseException as e:  # noqa
        res_q.put(("FAIL", idx, round(time.time() - t0, 1), f"{type(e).__name__}: {e}"))


def main():
    burst_n = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    warm_k = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    io_threads = int(sys.argv[3]) if len(sys.argv) > 3 else 1
    busy = bool(int(sys.argv[4])) if len(sys.argv) > 4 else False
    print(f">>> repro burst_N={burst_n} warm_K={warm_k} io_threads={io_threads} busy={busy}", flush=True)

    addr_q, up_q, res_q = CTX.Queue(), CTX.Queue(), CTX.Queue()
    stop_ev, ready_ev = CTX.Event(), CTX.Event()

    binder = CTX.Process(target=_binder_proc, args=(io_threads, addr_q, stop_ev, ready_ev))
    binder.start()
    ready_ev.wait(30)
    addr = addr_q.get(timeout=30)
    print(f">>> binder listening at {addr}", flush=True)

    warms = []
    for i in range(warm_k):
        p = CTX.Process(target=_warm_proc, args=(addr, i, busy, stop_ev, up_q))
        p.start()
        warms.append(p)
    for _ in range(warm_k):
        try:
            print(">>> warm:", up_q.get(timeout=30), flush=True)
        except Exception:
            print(">>> warm: (no report)", flush=True)
    time.sleep(1.0)  # let warm peers settle / start generating IO

    barrier = CTX.Barrier(burst_n)
    bursts = [CTX.Process(target=_burst_proc, args=(addr, i, barrier, res_q)) for i in range(burst_n)]
    for p in bursts:
        p.start()

    results = []
    for _ in range(burst_n):
        try:
            results.append(res_q.get(timeout=180))
        except Exception:
            results.append(("TIMEOUT", -1, -1, "no result in 180s"))

    ok = [r for r in results if r[0] == "ok"]
    fail = [r for r in results if r[0] != "ok"]
    print(f">>> RESULT: {len(ok)}/{burst_n} connected, {len(fail)} FAILED", flush=True)
    for r in fail[:20]:
        print(f">>>   FAIL idx={r[1]} after {r[2]}s: {r[3]}", flush=True)

    stop_ev.set()
    for p in [*bursts, *warms, binder]:
        p.join(timeout=10)
        if p.is_alive():
            p.kill()
    print(">>> DONE", flush=True)
    sys.exit(1 if fail else 0)


if __name__ == "__main__":
    main()
