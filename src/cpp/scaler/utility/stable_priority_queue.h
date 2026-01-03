
#pragma once

#include <cstdint>
#include <map>
#include <unordered_map>

namespace scaler {
namespace utility {

template <typename T>
struct StablePriorityQueue {
    using PriorityType = int64_t;
    using CounterType  = uint64_t;
    using MapKeyType   = std::pair<PriorityType, CounterType>;
    using ItemType     = std::pair<PriorityType, T>;

    std::unordered_map<T, MapKeyType> _locator;
    std::map<MapKeyType, T> _queue;
    CounterType _itemCounter;

    StablePriorityQueue(): _itemCounter {} {}

    constexpr uint64_t size() const { return _queue.size(); }

    void put(ItemType item)
    {
        const auto& [priority, data] = item;
        MapKeyType mapKey            = {priority, _itemCounter};
        _locator[data]               = mapKey;
        _queue[mapKey]               = std::move(data);
        ++_itemCounter;
    }

    std::pair<ItemType, bool> get()
    {
        ItemType res {};
        if (_queue.empty()) {
            return {res, false};
        }

        auto it        = _queue.begin();
        MapKeyType key = std::move(it->first);
        T data         = std::move(it->second);

        _queue.erase(it);
        _locator.erase(data);

        res.first  = key.first;
        res.second = std::move(data);
        return {res, true};
    }

    void remove(const T& data)
    {
        const auto it = _locator.find(data);
        if (it == _locator.end()) {
            return;
        }

        _queue.erase(it->second);
        _locator.erase(data);
    }

    void decreasePriority(const T& data)
    {
        auto it = _locator.find(data);
        if (it == _locator.end()) {
            return;
        }

        auto& key    = it->second;
        auto oldData = std::move(_queue[key]);
        _queue.erase(key);

        --key.first;
        key.second  = _itemCounter;
        _queue[key] = std::move(oldData);

        ++_itemCounter;
    }

    std::pair<ItemType, bool> maxPriorityItem() const
    {
        if (_queue.empty()) {
            return {{}, false};
        }
        auto kvit = _queue.cbegin();
        return {ItemType {kvit->first.first, kvit->second}, true};
    }
};

}  // namespace utility
}  // namespace scaler
