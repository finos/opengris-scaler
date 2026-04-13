:orphan:

Release Notes
=============

Version 2.0.14
--------------

.. rubric:: Major Changes

* **Documentation cleanup** across release-facing docs.

Version 2.0.12
--------------

.. rubric:: Major Changes

* **Added support for Python 3.14**.
* **Removed the pycapnp dependency**. Scaler now directly relies on the C++ Capnp library.
* **Standardized multiprocessing process creation** to consistently use spawn context.

Version 2.0.9
-------------

.. rubric:: Major Changes

* **Added AWS HPC Batch support** with related bug fixes and docs updates.
* **Improved scaling behavior** by reporting pending (launching) workers to prevent over-requesting.
* **Added and expanded examples/docs**, including ORB AWS EC2 quickstart, SwapCVA, XVA, and order-flow docs.

Version 2.0.7
-------------

.. rubric:: Major Changes

* Added CI **support for Intel macOS**.
* **Fixed macOS ARM release** CI issues.

Version 2.0.5
-------------

.. rubric:: Major Changes

* **Fixed Linux ARM build** issues.

Version 2.0.4
-------------

.. rubric:: Major Changes

* **Fixed Python 3.14 wheel build** issues.

Version 2.0.3
-------------

.. rubric:: Major Changes

* Added CI build, test, and wheel release **support for ARM macOS**.

Version 2.0.2
-------------

.. rubric:: Major Changes

* Added a YMQ magic-string check to **detect non-YMQ clients early**.

.. rubric:: Compatibility Notes

* Network handshake protocol has been updated. Older clients, worker and scheduler will not be able to connect to
  newer versions.

Version 2.0.1
-------------

.. rubric:: Major Changes

* **Object storage address advertisement behavior is corrected and more predictable**.


Version 2.0.0
-------------

Scaler 2.0.0 focuses on policy-driven scaling, worker manager consolidation,
and the YMQ-libuv networking/runtime transition.

.. rubric:: Major Changes

* **Scaling policy system reworked**: scaling controllers are stateless,
  REST-driven scaling control is removed, ``waterfall_v1`` is introduced,
  capability-aware behavior is improved, and the default scheduler scaling
  policy is now ``vanilla`` (with ``fixed_elastic`` removed).

* **Worker manager architecture reorganized**: worker manager lifecycle and
  control paths are unified across scheduler, configuration, and entry points,
  including stricter ``worker_manager_id`` handling.

* **New worker managers added**:

  * AWS Batch worker manager
  * ORB AWS EC2 worker manager

* **Worker manager implementations simplified**: fixed-native behavior is
  merged into native management paths, reducing overlap and simplifying
  deployment options.

* **YMQ-libuv becomes the standard network backend**: Python wrappers and
  connector sets are added, object storage and scheduler paths are aligned,
  and YMQ now uses libuv as the supported I/O backend.

* **Reliability and correctness improvements**: targeted fixes across asyncio,
  connector lifecycle, object storage handling, and YMQ error/overflow paths.

* **Web UI modernization**: the Web UI is rewritten and refined with follow-up
  fixes and usability improvements.

.. rubric:: Compatibility Notes

* ``Client.map()`` and ``Client.starmap()`` align with Python semantics
  introduced in recent releases; deprecated tuple-style ``map`` usage was
  removed from tests and examples.
* Ray compatibility remains available and aligned with current scheduler policy
  configuration patterns.
