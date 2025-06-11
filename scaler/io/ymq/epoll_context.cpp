#include "scaler/io/ymq/epoll_context.h"
#include <format>
#include "scaler/io/ymq/common.h"

void EpollContext::registerEventManager(EventManager& em) {
    epoll_event ev {
        .events = EPOLLOUT | EPOLLIN | EPOLLET,  // Edge-triggered
        .data   = {.ptr = &em},
    };

    epoll_fd.epoll_ctl(EPOLL_CTL_ADD, em._fd, &ev);
}

void EpollContext::removeEventManager(EventManager& em) {
    epoll_fd.epoll_ctl(EPOLL_CTL_DEL, em._fd, nullptr);
}

void EpollContext::executePendingFunctors() {}

void EpollContext::loop() {
    std::array<epoll_event, 1024> events;
    auto result = epoll_fd.epoll_wait(events.data(), 1024, -1);

    if (!result) {
        panic(std::format("Failed to epoll_wait(): {}", result.error()));
    };

    for (auto it = events.begin(); it != events.begin() + *result; ++it) {
        epoll_event current_event = *it;
        if (current_event.events & EPOLLERR) {
            // ...
        }

        auto* event = (EventManager*)current_event.data.ptr;
        // event->onEvents(current_event.events);
    }
    executePendingFunctors();
}

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
