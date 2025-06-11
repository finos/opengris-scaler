#pragma once

#include <map>
#include <memory>
#include <thread>

#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/event_loop.h"
#include "scaler/io/ymq/typedefs.h"

class IOSocket;

class EventLoopThread: public std::enable_shared_from_this<EventLoopThread> {
    using PollingContext = Configuration::PollingContext;
    std::jthread thread;
    std::map<std::string, IOSocket> identityToIOSocket;

public:
    EventLoop<PollingContext> eventLoop;
    // Why not make the class a friend class of IOContext?
    // Because the removeIOSocket method is a bit trickier than addIOSocket,
    // the IOSocket that is being removed will first remove every
    // MessageConnectionTCP managed by it from the EventLoop, before it removes
    // it self from ioSockets. return eventLoop.executeNow(createIOSocket());
    IOSocket* createIOSocket(std::string identity, IOSocketType socketType);

    void removeIOSocket(IOSocket* target);
    // EventLoop<PollingContext>& getEventLoop();
    // IOSocket* getIOSocketByIdentity(size_t identity);

    EventLoopThread(const EventLoopThread&)            = delete;
    EventLoopThread& operator=(const EventLoopThread&) = delete;
    // TODO: Revisit the default ctor
    EventLoopThread() = default;
};
