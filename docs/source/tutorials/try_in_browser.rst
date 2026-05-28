Try Scaler in your browser
==========================

The Scaler client also runs entirely inside your web browser via a
`JupyterLite <https://jupyterlite.readthedocs.io/>`_ build hosted with these
docs. The notebooks below open in a Pyodide kernel that has the Scaler client
pre-installed, so you can experiment with ``submit`` / ``send_object`` /
``map`` without installing anything locally.

The browser only runs the **client**. The Scaler **scheduler** and
**worker(s)** still run natively (typically on your laptop or a remote VM)
and the browser client connects to them over WebSockets.

Worker setup
------------

The in-browser kernel runs **CPython 3.13** (the Pyodide-bundled version), so
the workers your browser client talks to must also run CPython 3.13 -- the
``capnp`` protocol bindings rely on a matching CPython ABI. Mixing a 3.13
browser client with a 3.10 / 3.11 / 3.12 scheduler is unsupported and surfaces
as opaque capnp decoding errors.

The eight notebooks below need only ``numpy`` and ``scikit-learn`` on the
worker side; everything else they use is in the Python standard library.

.. code-block:: bash

    python3.13 -m venv .venv
    source .venv/bin/activate
    pip install opengris-scaler 'numpy<2.3' scikit-learn

The ``numpy<2.3`` cap matches the version Pyodide currently bundles -- this
avoids subtle binary-layout mismatches when arrays cross the wire.

Then start a small local cluster that accepts WebSocket connections from the
browser:

.. code-block:: bash

    scaler_cluster -n 4 ws://0.0.0.0:2345

or, equivalently, from a Python REPL on the same host:

.. code-block:: python

    from scaler import SchedulerClusterCombo

    cluster = SchedulerClusterCombo(address="ws://0.0.0.0:2345", n_workers=4)
    # ... keep the process alive while the browser client runs ...

Each notebook has two constants near the top that you edit before running --
``SCHEDULER_ADDRESS`` and ``OBJECT_STORAGE_ADDRESS`` -- so just point them at
your running cluster:

.. code-block:: python

    SCHEDULER_ADDRESS = "ws://127.0.0.1:2345"
    OBJECT_STORAGE_ADDRESS = None  # let the scheduler advertise it


Demo notebooks
--------------

Each notebook is intentionally **worker-heavy and client-light**: the browser
client only orchestrates a batch of independent tasks and aggregates a small
result, while the actual CPU work happens on the native workers.

.. list-table::
   :widths: 60 40
   :header-rows: 1

   * - Notebook
     - Open in browser
   * - :doc:`Parallel square roots (warm-up) <../gallery/parallel_sqrt>`
     - .. raw:: html

          <a class="try-in-browser" href="../lite/lab/index.html?path=parallel_sqrt.ipynb" target="_blank" rel="noopener">Open</a>
   * - :doc:`Heavy object reuse with send_object <../gallery/send_heavy_object>`
     - .. raw:: html

          <a class="try-in-browser" href="../lite/lab/index.html?path=send_heavy_object.ipynb" target="_blank" rel="noopener">Open</a>
   * - :doc:`Monte Carlo estimation of pi <../gallery/monte_carlo_pi>`
     - .. raw:: html

          <a class="try-in-browser" href="../lite/lab/index.html?path=monte_carlo_pi.ipynb" target="_blank" rel="noopener">Open</a>
   * - :doc:`Mandelbrot tile rendering <../gallery/mandelbrot_tiles>`
     - .. raw:: html

          <a class="try-in-browser" href="../lite/lab/index.html?path=mandelbrot_tiles.ipynb" target="_blank" rel="noopener">Open</a>
   * - :doc:`Segmented prime sieve <../gallery/prime_sieve>`
     - .. raw:: html

          <a class="try-in-browser" href="../lite/lab/index.html?path=prime_sieve.ipynb" target="_blank" rel="noopener">Open</a>
   * - :doc:`Word-count map-reduce <../gallery/word_count_mapreduce>`
     - .. raw:: html

          <a class="try-in-browser" href="../lite/lab/index.html?path=word_count_mapreduce.ipynb" target="_blank" rel="noopener">Open</a>
   * - :doc:`Image batch filter <../gallery/image_batch_filter>`
     - .. raw:: html

          <a class="try-in-browser" href="../lite/lab/index.html?path=image_batch_filter.ipynb" target="_blank" rel="noopener">Open</a>
   * - :doc:`Hyperparameter grid search (sklearn) <../gallery/sklearn_grid_search>`
     - .. raw:: html

          <a class="try-in-browser" href="../lite/lab/index.html?path=sklearn_grid_search.ipynb" target="_blank" rel="noopener">Open</a>


For the heavier real-world gallery notebooks (parfun, pargraph, XVA, etc.)
see :doc:`examples` -- those are best run against a native cluster from a
native Python install.

.. toctree::
   :hidden:

   ../gallery/parallel_sqrt
   ../gallery/send_heavy_object
   ../gallery/monte_carlo_pi
   ../gallery/mandelbrot_tiles
   ../gallery/prime_sieve
   ../gallery/word_count_mapreduce
   ../gallery/image_batch_filter
   ../gallery/sklearn_grid_search
