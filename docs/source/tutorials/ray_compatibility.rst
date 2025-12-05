.. _ray_compatibility:

Ray Compatibility Layer
=======================

Scaler provides a compatibility layer that allows developers familiar with the `Ray <https://www.ray.io/>`_ API to use Scaler with minimal code changes. This layer intercepts calls to `ray` functions and translates them to Scaler's backend.

How to Use
----------

To enable the Ray compatibility layer, simply import ``scaler.compat.ray`` in your application. This import patches the `ray` module, allowing you to use `ray`'s API as you normally would. The order of importing `ray` and `scaler.compat.ray` does not matter.

.. code-block:: python

    import ray

    # Import the compatibility layer to patch the Ray API
    import scaler.compat.ray

    # this is a no-op
    ray.init()

    # you can use this fn to configure scaler-specific options
    # you can pass the address of an existing cluster
    # otherwise a new scheduler cluster combo will be created on the current machine
    #
    # similarly to ray, this will implicitly be called on the first use of a remote function with no arguments
    scaler.compat.ray.init()

    @ray.remote
    def my_function():
        return 1

    future = my_function.remote()
    assert ray.get(future) == 1

    # this disconnects the client
    # and shuts down the combo, if it was created
    ray.shutdown()

Supported APIs
--------------

The compatibility layer supports a subset of the Ray API. Below is a list of the most important supported functions.

Core API
~~~~~~~~

*   ``ray.init()``: Initializes the Scaler cluster. It can be called without arguments to start a local cluster, or with an address to connect to an existing cluster.
*   ``ray.shutdown()``: Shuts down the Scaler cluster.
*   ``ray.is_initialized()``: Checks if the cluster is initialized.
*   ``@ray.remote``: Decorator to define a remote function (task). Note that decorating classes (for actors) is **not** supported.
*   ``ray.get()``: Retrieves the result of a remote function call.
*   ``ray.put()``: Puts an object into the object store.
*   ``ray.wait()``: Waits for a list of object references to be ready.
*   ``ray.cancel()``: Cancels a task.

Ray Utilities (`ray.util`)
~~~~~~~~~~~~~~~~~~~~~~~~~~

*   ``ray.util.as_completed()``: Returns an iterator that yields futures as they complete.
*   ``ray.util.map_unordered()``: Applies a remote function to a list of items and returns an iterator over the results in the order they complete.

Example Usage
-------------

The following example demonstrates how to use the Ray compatibility layer to run a simple task.

.. literalinclude:: ../../../tests/compat/test_ray_compat.py
   :language: python
   :start-after: def test_basic(self) -> None:
   :end-before: self.assertEqual(ray.get(ref), 7)

Unsupported Features
--------------------

While the compatibility layer aims to provide a seamless transition from Ray, not all features are supported. The most notable unsupported feature is **Ray Actors**. Decorating a class with ``@ray.remote`` will raise a ``NotImplementedError``.

Other `ray` modules and functions (e.g., `ray.actor`, `ray.timeline`) are patched to be no-ops or to raise a `NotImplementedError` to prevent unexpected behavior. If you rely on a specific Ray feature that is not listed in the supported APIs, it is likely not implemented.

How It Works
------------

The compatibility layer works by using `monkey-patching <https://en.wikipedia.org/wiki/Monkey_patch>`_. When ``scaler.compat.ray`` is imported, it replaces functions and modules in the `ray` package with Scaler's own implementations. This allows your existing code that uses `ray` to work with Scaler's backend without modification.
