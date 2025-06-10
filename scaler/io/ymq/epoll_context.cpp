
#include "scaler/io/ymq/epoll_context.h"

#include "scaler/io/ymq/event_manager.h"

void EpollContext::addFdToLoop(int fd, uint64_t events, EventManager* manager) {
    epoll_event event {};
    event.events   = (int)events & (EPOLLIN | EPOLLOUT | EPOLLET);
    event.data.ptr = (void*)manager;
    epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &event);
}

void EpollContext::executePendingFunctors() {}

void EpollContext::loop() {
    std::array<epoll_event, 1024> events;
    int n = epoll_wait(epfd, events.data(), 1024, 0);
    for (auto it = events.begin(); it != events.begin() + n; ++it) {
        epoll_event current_event = *it;
        if (current_event.events & EPOLLERR) {
            // ...
        }

        auto* event = (EventManager*)current_event.data.ptr;
        event->onEvents(current_event.events);
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
