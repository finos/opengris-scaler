"""
This example was copied from https://docs.ray.io/en/latest/ray-core/examples/web_crawler.html

Like in `highly_parallel.py`, only minimal changes are needed for the example to work on Scaler.
"""

import time
from typing import Iterable, Set

import ray
import requests
from bs4 import BeautifulSoup, Tag

# changed line 1/2
import scaler.compat.ray


def extract_links(elements: Iterable[Tag], base_url: str, max_results: int = 100) -> Set[str]:
    links = []
    for e in elements:
        url = str(e["href"])
        if "https://" not in url:
            url = base_url + url
        if base_url in url:
            links.append(url)
    return set(links[:max_results])


def find_links(start_url: str, base_url: str, depth: int = 2) -> Set[str]:
    if depth == 0:
        return set()

    page = requests.get(start_url)
    soup = BeautifulSoup(page.content, "html.parser")
    elements = soup.find_all("a", href=True)
    links = extract_links(elements, base_url)

    for url in links:
        new_links = find_links(url, base_url, depth - 1)
        links = links.union(new_links)
    return links


base = "https://docs.ray.io/en/latest/"
docs = base + "index.html"


def main():
    # initialize the scaler cluster with a fixed number of workers to prevent
    # excessive memory usage during import in CI environments
    scaler.compat.ray.scaler_init(n_workers=4)

    @ray.remote
    def find_links_task(start_url: str, base_url: str, depth: int = 2) -> Set[str]:
        return find_links(start_url, base_url, depth)

    start = time.time()
    find_links(docs, base)
    serial_elapsed = time.time() - start

    start = time.time()
    [find_links_task.remote(f"{base}{lib}/index.html", base) for lib in ["", "", "", "rllib", "tune", "serve"]]
    parallel_elapsed = time.time() - start

    print(f"serial time: {serial_elapsed:.2}s; parallel time: {parallel_elapsed:.2}s")


if __name__ == "__main__":
    main()

    # changed line 2/2
    ray.shutdown()
