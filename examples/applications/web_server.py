import nest_asyncio
import sys
import time
import grequests
import requests
import asyncio
import multiprocessing
from psutil import cpu_count
from timeit import default_timer as timer
from quart import Quart
from scaler import SchedulerClusterCombo, Client

nest_asyncio.apply()


def server(scaled: bool):
    app = Quart(__name__)

    if scaled:
        cluster = SchedulerClusterCombo(n_workers=cpu_count())
        client = Client(address=cluster.get_address())

        @app.route("/")
        async def root():
            future = client.submit(requests.get, "https://example.com")
            response: requests.Response = await asyncio.wrap_future(future)
            return response.text

        app.run(loop=asyncio.get_event_loop(), use_reloader=False)
        cluster.shutdown()
    else:
        @app.route("/")
        def root():
            return requests.get("https://example.com").text

        app.run(use_reloader=False)


def client() -> float:
    start = timer()
    requests = [grequests.get("http://127.0.0.1:5000") for _ in range(500)]
    _ = grequests.map(requests)
    return timer() - start


def client_multiprocess(result: multiprocessing.Queue):
    t = client()
    result.put(t)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("no args provided, speed testing")

        server_ = multiprocessing.Process(target=server, args=(False,))
        server_.start()

        result: multiprocessing.Queue = multiprocessing.Queue()
        client_ = multiprocessing.Process(target=client_multiprocess, args=(result,))
        client_.start()
        client_.join()
        server_.kill()
        server_.join()
        nonscaled = result.get()

        server_ = multiprocessing.Process(target=server, args=(True,))
        server_.start()
        time.sleep(5)
        client_ = multiprocessing.Process(target=client_multiprocess, args=(result,))
        client_.start()
        client_.join()
        server_.kill()
        server_.join()
        scaled = result.get()
        print(f"Non-scaled time: {nonscaled:.2f}s")
        print(f"Scaled time: {scaled:.2f}s")
        print(f"Speedup: {nonscaled / scaled:.2f}x")
    elif len(sys.argv) == 2 and sys.argv[1] == "server" and sys.argv[2] == "scaled":
        server(scaled=True)
    elif len(sys.argv) == 2 and sys.argv[1] == "server" and sys.argv[2] == "unscaled":
        server(scaled=False)
    elif len(sys.argv) == 2 and sys.argv[1] == "client":
        elapsed = client()

        print(f"Time: {elapsed:.2f}s")
    else:
        print("Usage: web_server.py server|client [scaled|unscaled]")
        sys.exit(1)
