Try in your browser
===================

The Scaler client runs in your browser on a `JupyterLite
<https://jupyterlite.readthedocs.io/>`_ (Pyodide) kernel that already has the
client installed. The scheduler and worker managers still run natively, and the
browser can only reach them over a ``ws://`` address.

The quickest way to get a scheduler and worker managers listening on ``ws://``
is the `Launchpad </scaler/launchpad/>`_: hit *Launch*, then paste the address
it gives you into a notebook's ``SCHEDULER_ADDRESS`` and run all cells.

.. note::

   Workers must run Python 3.13 to match the in-browser (Pyodide) kernel, with
   ``numpy`` (pinned ``<2.3`` to match Pyodide) and ``scikit-learn`` installed
   on the worker side.

Demo notebooks
--------------

Each demo is **worker-heavy and client-light**: the browser orchestrates a
batch of independent tasks while the actual CPU work happens on the workers.

Read the write-up for any example, or launch it straight into the in-browser
JupyterLite notebook:

.. raw:: html

   <ul class="lite-demos">
     <li>
       <a class="lite-demo" href="../gallery/parallel_sqrt.html">Parallel square roots (warm-up)</a>
       <span class="lite-sep">&mdash;</span>
       <a class="lite-open" href="../lite/lab/index.html?path=parallel_sqrt.ipynb" target="_blank" rel="noopener">open in browser &#8594;</a>
     </li>
     <li>
       <a class="lite-demo" href="../gallery/send_heavy_object.html">Heavy object reuse with send_object</a>
       <span class="lite-sep">&mdash;</span>
       <a class="lite-open" href="../lite/lab/index.html?path=send_heavy_object.ipynb" target="_blank" rel="noopener">open in browser &#8594;</a>
     </li>
     <li>
       <a class="lite-demo" href="../gallery/monte_carlo_pi.html">Monte Carlo estimation of pi</a>
       <span class="lite-sep">&mdash;</span>
       <a class="lite-open" href="../lite/lab/index.html?path=monte_carlo_pi.ipynb" target="_blank" rel="noopener">open in browser &#8594;</a>
     </li>
     <li>
       <a class="lite-demo" href="../gallery/mandelbrot_tiles.html">Mandelbrot tile rendering</a>
       <span class="lite-sep">&mdash;</span>
       <a class="lite-open" href="../lite/lab/index.html?path=mandelbrot_tiles.ipynb" target="_blank" rel="noopener">open in browser &#8594;</a>
     </li>
     <li>
       <a class="lite-demo" href="../gallery/prime_sieve.html">Segmented prime sieve</a>
       <span class="lite-sep">&mdash;</span>
       <a class="lite-open" href="../lite/lab/index.html?path=prime_sieve.ipynb" target="_blank" rel="noopener">open in browser &#8594;</a>
     </li>
     <li>
       <a class="lite-demo" href="../gallery/word_count_mapreduce.html">Word-count map-reduce</a>
       <span class="lite-sep">&mdash;</span>
       <a class="lite-open" href="../lite/lab/index.html?path=word_count_mapreduce.ipynb" target="_blank" rel="noopener">open in browser &#8594;</a>
     </li>
     <li>
       <a class="lite-demo" href="../gallery/image_batch_filter.html">Image batch filter</a>
       <span class="lite-sep">&mdash;</span>
       <a class="lite-open" href="../lite/lab/index.html?path=image_batch_filter.ipynb" target="_blank" rel="noopener">open in browser &#8594;</a>
     </li>
     <li>
       <a class="lite-demo" href="../gallery/sklearn_grid_search.html">Hyperparameter grid search (sklearn)</a>
       <span class="lite-sep">&mdash;</span>
       <a class="lite-open" href="../lite/lab/index.html?path=sklearn_grid_search.ipynb" target="_blank" rel="noopener">open in browser &#8594;</a>
     </li>
   </ul>

For heavier real-world notebooks see :doc:`examples` -- 
these are too heavy for a browser kernel and are best
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
