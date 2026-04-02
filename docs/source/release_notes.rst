Release Notes
=============

Version 2.0.0
-------------

Scaler 2.0.0 focuses on policy-driven scaling, worker manager consolidation,
and the YMQ-libuv networking/runtime transition.

Major Changes
-------------

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

Compatibility Notes
-------------------

* ``Client.map()`` and ``Client.starmap()`` align with Python semantics
  introduced in recent releases; deprecated tuple-style ``map`` usage was
  removed from tests and examples.
* Ray compatibility remains available and aligned with current scheduler policy
  configuration patterns.
