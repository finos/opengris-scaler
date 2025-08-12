# YMQ Python Interfce TODO

## Done

- Create RAII abstraction for reference counting
- Propagate errors to futures in more situations
  - unify result setting and error raising fns
- Replace latch-check-signal loop with wakeupfd and poll

## Todo

- Investigate zerocopy for constructing Bytes
- The latch-check-signals loops are bad, investigate replacing with pysignal_setwakeupfd or a signalfd
- Put everything in scaler::ymq namespace
- Migrate pub/sub sockets back to ZMQ
- Why do the Bytes need to be incref'd in recv?
