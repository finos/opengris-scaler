#include "scaler/io/ymq/event_manager.h"

void EventManager::addToEventLoop() {
    _eventLoopThread.getEventLoop().registerEventManager(*this);
}

void EventManager::removeFromEventLoop() {
    _eventLoopThread.getEventLoop().removeEventManager(*this);
}
