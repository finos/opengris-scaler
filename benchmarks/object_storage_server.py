import os
import time
from typing import Callable

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import Normalize

from scaler.client.client import Client
from scaler.cluster.combo import SchedulerClusterCombo


def plot_delta_with_errors(delta, errors, n_objects, object_size, out_path):
    """
    delta: 2D numpy array (shape: len(n_objects) x len(object_size))
    errors: boolean 2D array same shape, True where a run failed
    n_objects: 1D array of row labels
    object_size: 1D array of column labels (in bytes)
    out_path: filename to save the PNG
    """
    h, w = delta.shape
    rgba = np.ones((h, w, 4), dtype=float)  # default white

    # Masks
    pos_mask = (delta > 0) & (~errors)
    neg_mask = (delta < 0) & (~errors)
    zero_mask = (delta == 0) & (~errors)
    err_mask = errors

    # --- Color computation ---
    if np.any(pos_mask):
        max_pos = float(np.nanmax(delta[pos_mask])) or 1.0
        norm_pos = Normalize(vmin=0.0, vmax=max_pos, clip=True)
        rgba[pos_mask] = plt.get_cmap("Reds")(norm_pos(delta[pos_mask]))

    if np.any(neg_mask):
        min_neg = float(np.nanmin(delta[neg_mask])) or -1.0
        norm_neg = Normalize(vmin=min_neg, vmax=0.0, clip=True)
        rgba[neg_mask] = plt.get_cmap("Greens_r")(norm_neg(delta[neg_mask]))

    rgba[zero_mask] = (1.0, 1.0, 1.0, 1.0)
    rgba[err_mask] = (0.6, 0.6, 0.6, 1.0)

    # --- Plot setup ---
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.imshow(rgba, origin="lower", extent=[0, w, 0, h], interpolation="nearest")

    # Grid lines
    for x in range(w + 1):
        ax.axvline(x, color="black", linewidth=0.5)
    for y in range(h + 1):
        ax.axhline(y, color="black", linewidth=0.5)

    # Ticks
    ax.set_xticks(np.arange(w) + 0.5)
    ax.set_xticklabels(
        [f"{s//1024} KB" if s < 1024**2 else f"{s//1024**2} MB" for s in object_size], rotation=45, ha="right"
    )
    ax.set_yticks(np.arange(h) + 0.5)
    ax.set_yticklabels(n_objects)
    ax.set_xlabel("Object Size")
    ax.set_ylabel("Number of Objects")
    ax.set_title("Object Storage Server Performance, ZMQ+TCP vs YMQ backends")

    # --- Write cell values ---
    for i in range(h):
        for j in range(w):
            if errors[i, j]:
                text = "ERR"
                color = "black"
            else:
                val = delta[i, j]
                if np.isnan(val):
                    text, color = "", "black"
                else:
                    text = f"{val:.2f}"
                    color = "black" if abs(val) < 0.5 else "white"  # contrast rule
            ax.text(j + 0.5, i + 0.5, text, ha="center", va="center", fontsize=8, color=color, fontweight="bold")

    # --- Legend ---
    red_patch = mpatches.Patch(color=plt.get_cmap("Reds")(0.6), label="Δ > 0 (ymq is slower)")
    green_patch = mpatches.Patch(color=plt.get_cmap("Greens")(0.6), label="Δ < 0 (ymq is faster)")
    err_patch = mpatches.Patch(color=(0.6, 0.6, 0.6), label="Error (gray)")
    ax.legend(
        handles=[red_patch, green_patch, err_patch],
        bbox_to_anchor=(1.02, 1),
        loc="upper left",
        title="Delta (seconds)",
        title_fontsize=11,
        fontsize=9,
    )

    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close(fig)


def timed_execution(test: Callable[[Client], None], port: int, *args, **kwargs) -> float:
    address = f"tcp://127.0.0.1:{port}"
    combo = SchedulerClusterCombo(1, address)
    client = Client(address)

    # warm up
    client.submit(lambda: 0).result()
    start = time.perf_counter()

    test(client, *args, **kwargs)

    # ensure all objects have been sent
    client.submit(lambda: 0).result()
    assert client._object_buffer._pending_objects.__len__() == 0

    elapsed = time.perf_counter() - start

    client.disconnect()
    combo.shutdown()

    return elapsed


def send_objects(client: Client, n_objects: int, object_size: int) -> None:
    payload = b"." * object_size
    for _ in range(n_objects):
        _ = client.send_object(payload)


def main():
    n_objects = np.array([100, 1000, 10_000, 20_000, 30_000, 40_000])
    object_size = np.array([1024, 10 * 1024, 20 * 1024])

    ymq_times = np.zeros((len(n_objects), len(object_size)))
    zmq_times = np.zeros((len(n_objects), len(object_size)))
    errors = np.zeros_like(ymq_times, dtype=bool)

    port = 2345

    for i, n in enumerate(n_objects):
        for j, sz in enumerate(object_size):
            print(f"Executing case: {n} objects of {sz} bytes")

            try:
                os.environ["SCALER_NETWORK_BACKEND"] = "ymq"
                ymq_times[i, j] = timed_execution(send_objects, port, n, sz)
                port += 1

                os.environ["SCALER_NETWORK_BACKEND"] = "tcp_zmq"
                zmq_times[i, j] = timed_execution(send_objects, port, n, sz)
                port += 1
            except KeyboardInterrupt:
                port += 1
                ymq_times[i, j] = np.nan
                zmq_times[i, j] = np.nan
                errors[i, j] = True

    delta = ymq_times / zmq_times

    print(delta)

    plot_delta_with_errors(delta, errors, n_objects, object_size, out_path="plot.png")
    print("Saved plot.png")


if __name__ == "__main__":
    main()
