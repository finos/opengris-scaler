# YMQ Python Interfce TODO

- Error handling for callback inside `future_set_result()`
  - Unify set result and raise exception fns?
- Investigate zerocopy for constructing Bytes
- The latch-check-signals loops are bad, investigate replacing with pysignal_setwakeupfd or a signalfd
