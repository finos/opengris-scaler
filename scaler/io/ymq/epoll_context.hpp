#pragma once

// C++
#include <functional>

// First-party
#include "event_manager.hpp"

struct EpollContext {
    using Function   = std::function<void()>;  // TBD
    using TimeStamp  = int;                    // TBD
    using Identifier = int;                    // TBD
    void registerCallbackBeforeLoop(EventManager*);
    void loop() {
        for (;;) {
            // EXAMPLE
            // epoll_wait;
            // for each event that is returned to the caller {
            //     cast the event back to EventManager
            //     if this event is an eventfd {
            //         func = queue front
            //         func()
            //     } else if this event is an timerfd {
            //         if it is oneshot then execute once
            //         if it is multishot then execute and rearm timer
            //     } else {
            //         eventmanager.revent = events return by epoll
            //         eventmanager.on_event() ,
            //         where as on_event is set differently for tcpserver, tcpclient, and tcpconn

            //         they are defined something like:
            //         tcpserver.on_event() {
            //             accept the socket and generate a new tcpConn or handle err
            //             this.ioSocket.addNewConn(tcpConn)
            //         }
            //         tcpclient.on_event() {
            //             connect the socket and generate a new tcpConn
            //             this.ioSocket.addNewConn(tcpConn)
            //             if there need retry {
            //                 close this socket
            //                 this.eventloop.executeAfter(the time you want from now)
            //             }
            //         }
            //         tcpConn.on_event() {
            //             read everything you can to the buffer
            //             write everything you can to write the remote end
            //             if tcpconn.ioSocket is something special, for example dealer
            //             tcpConn.ioSocket.route to corresponding tcpConn
            //         }
            //
            //     }

            // }
        }
    }
    void stop();

    void executeNow(Function func);
    void executeLater(Function func, Identifier identifier);
    void executeAt(TimeStamp, Function, Identifier identifier);
    void cancelExecution(Identifier identifier);

    // int epoll_fd;
    // int connect_timer_tfd;
    // std::map<int, EventManager*> monitoringEvent;
    // bool timer_armed;
    // // NOTE: Utility functions, may be defined otherwise
    // void ensure_timer_armed();
    // void add_epoll(int fd, uint32_t flags, EpollType type, void* data);
    // void remove_epoll(int fd);
    // EpollData* epoll_by_fd(int fd);
};
