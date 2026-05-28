Try Scaler in your browser
==========================

The Scaler client runs inside your browser via a
`JupyterLite <https://jupyterlite.readthedocs.io/>`_ kernel hosted with these
docs. The notebooks below open in a Pyodide kernel that already has the
Scaler client installed -- no local setup required to run the client.

The scheduler and worker(s) still run natively. Point each notebook at your
own cluster by editing the two constants near the top:

.. code-block:: python

    SCHEDULER_ADDRESS = "ws://127.0.0.1:2345"
    OBJECT_STORAGE_ADDRESS = None  # let the scheduler advertise it

Only ``ws://`` addresses are reachable from the browser.

Running a cluster for the browser client
----------------------------------------

You need a scheduler and at least one worker listening on a WebSocket
address. Three ways to get one:

* **Local laptop / VM**: spin up a small cluster yourself --
  see :doc:`installation` and :doc:`quickstart` for the install steps and
  ``scaler_cluster`` invocation. Use ``ws://0.0.0.0:2345`` so the browser can
  connect.
* **One-click EC2**: the `Launchpad </launchpad/>`_ provisions a scheduler
  and a pool of workers on AWS EC2 for you. Hit *Launch*, paste the resulting
  ``ws://`` address into a notebook, and run.
* **Bring your own**: any cluster fronted by a worker manager
  (see :doc:`worker_managers/index`) works, as long as the scheduler is
  reachable over ``ws://``.

.. note::

   The in-browser kernel is **CPython 3.13** (Pyodide-bundled). Workers must
   run the same major/minor Python version so the ``capnp`` ABI matches
   (patch version does not matter). The demo notebooks only need ``numpy``
   (pin ``<2.3`` to match Pyodide) and ``scikit-learn`` on the worker side,
   on top of the standard library.

Demo notebooks
--------------

Each demo is **worker-heavy and client-light**: the browser orchestrates a
batch of independent tasks while the actual CPU work happens on the workers.

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

For heavier real-world gallery notebooks (parfun, pargraph, XVA, ...) see
:doc:`examples` -- those are too heavy for a browser kernel and are best
run from a native Python client.

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
