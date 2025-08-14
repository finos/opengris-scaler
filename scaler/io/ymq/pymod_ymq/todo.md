# YMQ Python Interfce TODO

## Done

- Create RAII abstraction for reference counting
- Propagate errors to futures in more situations
  - unify result setting and error raising fns
- Replace latch-check-signal loop with wakeupfd and poll
- Migrate pub/sub sockets back to ZMQ

## Todo

- Investigate zerocopy for constructing Bytes
- Put everything in scaler::ymq namespace
- Why do the Bytes need to be incref'd in recv?
