#pragma once

#include <list>
#include <unordered_map>

namespace scaler {
namespace utility {

template <typename T>
struct IndexedQueue {
    using iterator = std::list<T>::iterator;
    std::list<T> _list;
    std::unordered_map<T, iterator> _map;

    bool contains(const T& item) noexcept { return _map.find(item) != _map.end(); }
    size_t size() noexcept { return _map.size(); }

    bool put(T item) noexcept
    {
        auto it = _map.find(item);
        if (it != _map.end()) {
            return false;
        }
        _list.push_front(item);
        _map.insert({std::move(item), _list.begin()});
        return true;
    }

    std::pair<T, bool> get() noexcept
    {
        if (_list.empty()) {
            return {{}, false};
        }
        T res = std::move(_list.back());
        _list.pop_back();
        _map.erase(res);

        return {std::move(res), true};
    }

    bool remove(const T& item) noexcept
    {
        auto it = _map.find(item);
        if (it == _map.end()) {
            return false;
        }
        _list.erase(it->second);
        _map.erase(it);
        return true;
    }
};

}  // namespace utility
}  // namespace scaler
