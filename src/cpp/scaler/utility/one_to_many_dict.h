#pragma once

#include <unordered_map>
#include <unordered_set>

namespace scaler {
namespace utility {

template <typename K, typename V>
struct OneToManyDict {
    std::unordered_map<K, std::unordered_set<V>> _keyToValues;
    std::unordered_map<V, K> _valueToKey;
    using IterType        = decltype(_keyToValues)::iterator;
    using BucketCountType = decltype(_keyToValues)::size_type;

    bool contains(const K& key) { return _keyToValues.contains(key); }

    const std::unordered_map<K, std::unordered_set<V>>& keys() { return _keyToValues; }
    const std::unordered_map<V, K>& values() { return _valueToKey; }

    bool add(const K& key, const V& value)
    {
        if (_valueToKey.contains(value) && _valueToKey[value] != key) {
            return false;
        }

        _valueToKey[value] = key;
        _keyToValues[key].insert(value);
        return true;
    }

    bool hasKey(const K& key) { return _keyToValues.contains(key); }
    bool hasValue(const V& value) { return _valueToKey.contains(value); }

    const K* getKey(const V& value)
    {
        auto it = _valueToKey.find(value);
        return it == _valueToKey.end() ? nullptr : &it->second;
    }

    const std::unordered_set<V>* getValues(const K& key)
    {
        auto it = _keyToValues.find(key);
        return it == _keyToValues.end() ? nullptr : &it->second;
    }

    std::pair<std::unordered_set<V>, bool> removeKey(const K& key)
    {
        auto it = _keyToValues.find(key);
        if (it == _keyToValues.end()) {
            return {{}, false};
        }

        for (const auto& value: it->second) {
            _valueToKey.erase(value);
        }

        auto res = std::move(it->second);
        _keyToValues.erase(it);
        return {std::move(res), true};
    }

    std::pair<K, bool> removeValue(const V& value)
    {
        auto it = _valueToKey.find(value);
        if (it == _valueToKey.end()) {
            return {{}, false};
        }

        auto key = std::move(it->second);
        _valueToKey.erase(it);

        auto kvit = _keyToValues.find(key);
        if (kvit == _keyToValues.end()) {
            return {{}, false};
        }

        kvit->second.erase(value);

        if (kvit->second.size() == 0) {
            _keyToValues.erase(kvit);
        }

        return {std::move(key), true};
    }

    std::pair<IterType, BucketCountType> safeKeyToValueBegin() noexcept
    {
        return {_keyToValues.begin(), _keyToValues.bucket_count()};
    }

    std::pair<IterType, bool> safeKeyToValueNext(IterType iter, BucketCountType bucketCount) const noexcept
    {
        return {++iter, _keyToValues.bucket_count() == bucketCount};
    }

    bool keyToValueIteratorValid(BucketCountType bucketCount) const noexcept
    {
        return bucketCount == _keyToValues.bucket_count();
    }
};

}  // namespace utility
}  // namespace scaler
