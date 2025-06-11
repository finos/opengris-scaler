#include "scaler/io/ymq/tcp_server.h"

#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <memory>

#include "scaler/io/ymq/event_manager.h"

static int create_and_bind_socket() {
    int server_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (server_fd == -1) {
        perror("socket");
        return -1;
    }

    sockaddr_in addr {};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(8080);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
        perror("bind");
        close(server_fd);
        return -1;
    }

    if (listen(server_fd, SOMAXCONN) == -1) {
        perror("listen");
        close(server_fd);
        return -1;
    }

    return server_fd;
}

TcpServer::TcpServer(std::shared_ptr<EventLoopThread> eventLoop): eventLoop(eventLoop) {
    eventManager = std::make_unique<EventManager>(EventManager());
    serverFd     = create_and_bind_socket();
}

void TcpServer::onCreated() {
    printf("TcpServer::onAdded()\n");
    eventLoop->eventLoop.registerEventManager(*this->eventManager.get());
}
