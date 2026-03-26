Disconnect Client
=================

Shows how to disconnect a client session, clear client resources, and optionally stop the cluster.

.. literalinclude:: ../../../../examples/disconnect_client.py
   :language: python

What the example does:

* Starts a local cluster and creates a client.
* Calls :py:func:`~Client.clear()` to cancel unfinished tasks and clear client-owned resources.
* Calls :py:func:`~Client.disconnect()` to close only this client session.

Use ``disconnect`` when you want to close one client without stopping the scheduler.

Shutdown behavior
-----------------

By default, the scheduler runs in protected mode. See :ref:`protected <protected>` for details.

If protected mode is disabled, clients can stop the cluster by calling :py:func:`~Client.shutdown()`.
