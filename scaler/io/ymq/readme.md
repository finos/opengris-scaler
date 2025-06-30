
# YMQ

Welcome. This file contains schedule for each day, for each person.

Each person maintains a todo and done list.

## gxu

### DONE

- CMake integration, generate C++ stuff in build dir
- Basic coroutine API
- develop the so-called coro_context and their utility DEAD
- write up interfaces(not impl) that uses coroutine DEAD

## DONE:
 - Remember (or remove) the IOSocketIdentity of closed connection
 - Basic coroutine API (DEAD)
 - develop the so-called coro_context and their utility (DEAD)
 - write up interfaces(not impl) that uses coroutine (DEAD)
 - Use unified file path (only include dir is project dir)
 - Start organize files that they can be compiled in one go
 - Write delayed execution utility
 - Write timed execution utility
 - IOSocket exchange identity on connected
 - General message passing assuming user passed in Message with header
 - Guaranteed message delivery
 - Retry on disconnect
 - Delete fd from eventloop on disconnect
 - Put Recv on iosocket level because user don't know which connection it should recv from
 - cleanup: IOSocket destructor should release resources bound to it
 - cleanup: Clarify Bytes ownership of send/recv messages
 - Provide connect(str) function that parse the string to a sockaddr.
 - make connectTo takes a callback
 - Implement bind function with interface address (str) and callback
 - cleanup: report error when no connection with desired identity presents
 - cleanup: Give user more control about port/addr
 - test the project (now working when user kills the program)
 - cleanup: Provide actual remote sockaddr in the connection class
 - test the user provided callback logic and think about sync issue
 - connect do not happen twice, monitor for read/write instead
 - remove the end of each loop hook, this is stupid
 -
 - Per action cancellation
 - refactor: Ownership, Public/Private, destructor
 -
 -
 - LEAVE A FEW BLANKS HERE TO AVOID CONFLICT

## TODO:
 - CRTP, and so on
 -
 - cleanup: Change InterruptiveConcurrentQueue behavior (use FileDescriptor class)
 - think about this function call onConnectionIdentityReceived etc. It's way too intrusive.
 - make IO with send/recv msg
 - test the abnormal breakup (break up due to network issue instead of killing)
 -
 -
 - cleanup: Error handling
 -
 - cleanup: Do not constraint thee size of identity (current maximum being 128-8 bytes)
 - cleanup: Do not use std::string as identity type
 -
 - Provide a vector-like data structure that can release memory to achieve zc
 -
 -
 -
 - LEAVE A FEW BLANKS HERE TO AVOID CONFLICT


## magniloquency
=======
 - support for cancel of execution
 -
 -
 -
 -
 - LEAVE A FEW BLANKS HERE TO AVOID CONFLICT


### DONE

- CPython module stub

### TODO

- ?
