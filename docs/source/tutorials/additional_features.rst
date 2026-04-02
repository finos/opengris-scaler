Monitoring
==========

Scaler comes with a number of features that can be used to monitor and profile tasks, and customize behavior.

Scaler Top (Monitoring)
-----------------------

Top is a monitoring tool that allows you to see the status of the Scaler.
The scheduler prints an address to the logs on startup that can be used to connect to it with the `scaler_top` CLI command:

.. code:: bash

    scaler_top ipc:///tmp/0.0.0.0_8516_monitor

Which will show an interface similar to the standard Linux `top` command:

.. code:: console

   scheduler        | task_manager        |     scheduler_sent         | scheduler_received
         cpu   0.0% |   unassigned      0 |      HeartbeatEcho 283,701 |          Heartbeat 283,701
         rss 130.1m |      running      0 |     ObjectResponse     233 |      ObjectRequest     215
                    |      success 53,704 |           TaskEcho  53,780 |               Task  53,764
                    |       failed     14 |               Task  54,660 |         TaskResult  53,794
                    |     canceled     48 |         TaskResult  53,766 |  DisconnectRequest      21
                    |    not_found     14 |      ObjectRequest     366 |         TaskCancel      60
                                          | DisconnectResponse      21 |    BalanceResponse      15
                                          |         TaskCancel      62 |          GraphTask       6
                                          |     BalanceRequest      15 |
                                          |    GraphTaskResult       6 |
   -------------------------------------------------------------------------------------------------
   Shortcuts: worker[n] agt_cpu[C] agt_rss[M] cpu[c] rss[m] free[f] sent[w] queued[d] lag[l]

   Total 7 worker(s)
                      worker agt_cpu agt_rss [cpu]    rss free sent queued   lag ITL |    client_manager
   2732890|sd-1e7d-dfba|d26+    0.5%  111.8m  0.5% 113.3m 1000    0      0 0.7ms 100 |
   2732885|sd-1e7d-dfba|56b+    0.0%  111.0m  0.5% 111.2m 1000    0      0 0.7ms 100 | func_to_num_tasks
   2732888|sd-1e7d-dfba|108+    0.0%  111.7m  0.5% 111.0m 1000    0      0 0.6ms 100 |
   2732891|sd-1e7d-dfba|149+    0.0%  113.0m  0.0% 112.2m 1000    0      0 0.9ms 100 |
   2732889|sd-1e7d-dfba|211+    0.5%  111.7m  0.0% 111.2m 1000    0      0   1ms 100 |
   2732887|sd-1e7d-dfba|e48+    0.5%  112.6m  0.0% 111.0m 1000    0      0 0.9ms 100 |
   2732886|sd-1e7d-dfba|345+    0.0%  111.5m  0.0% 112.8m 1000    0      0 0.8ms 100 |


* `scheduler` section shows the scheduler's resource usage
* `task_manager` section shows the status of tasks
* `scheduler_sent` section counts the number of each type of message sent by the scheduler
* `scheduler_received` section counts the number of each type of message received by the scheduler
* `worker` section shows worker details, you can use shortcuts to sort by columns, and the * in the column header shows which column is being used for sorting

  * `agt_cpu/agt_rss` means cpu/memory usage of the worker agent
  * `cpu/rss` means cpu/memory usage of the worker
  * `free` means number of free task slots for the worker
  * `sent` means how many tasks scheduler sent to the worker
  * `queued` means how many tasks worker received and enqueued
  * `lag` means the latency between scheduler and the worker
  * `ITL` means is debug information

    * `I` means processor initialized
    * `T` means have a task or not
    * `L` means task lock


Additional client-facing feature guides have been consolidated into :doc:`scaler_client`.


Scaler Web GUI
--------------

Scaler also provides a browser-based monitoring dashboard through ``scaler_gui``.
It subscribes to the scheduler monitor stream and serves a real-time web UI over HTTP.

.. note::
   The GUI requires optional dependencies. Install with ``pip install opengris-scaler[gui]``.

Start the GUI by pointing it at the scheduler monitor address:

.. code:: bash

    scaler_gui tcp://127.0.0.1:6380 --gui-address 127.0.0.1:50001

Open ``http://127.0.0.1:50001`` in your browser.

What the Web GUI shows:

* **Live**: scheduler metrics, worker manager summary, and worker-level metrics (CPU/RSS/free/sent/queued/lag/ITL).
* **Task Log**: recent task lifecycle updates (running/success/failure/canceled), duration, peak memory, and capabilities.
* **Worker Task Stream**: a timeline by worker with capability colors and status overlays (failed and canceled patterns).
* **Memory Usage**: rolling cluster memory chart derived from task profiling metadata.
* **Worker Processors**: manager-grouped view of processor-level CPU/RSS and state flags (initialized, has task, suspended).

Interactive behavior:

* Uses WebSocket updates with auto-reconnect if the browser temporarily disconnects.
* Sends a full current snapshot on connect, then incremental updates in short batches.
* Supports runtime settings for stream window length (5/10/30 minutes) and memory chart scale (linear/log).
