#pragma once

#include <unordered_map>
#include <unordered_set>

namespace scaler {
namespace utility {

template <typename K, typename V>
struct KeyValueDictSet {
    std::unordered_map<K, std::unordered_set<V>> _keyToValueSet;

    auto cbegin() const { return _keyToValueSet.cbegin(); }
    auto cend() const { return _keyToValueSet.cend(); }
    auto begin() { return _keyToValueSet.begin(); }
    auto end() { return _keyToValueSet.end(); }
    const auto begin() const { return cbegin(); }
    const auto end() const { return cend(); }

    bool contains(const K& key) { return _keyToValueSet.contains(key); }

    const std::unordered_map<K, std::unordered_set<V>>& items() { return _keyToValueSet; }

    void add(K key, V value) { _keyToValueSet[key].insert(value); }

    std::pair<std::unordered_set<V>*, bool> getValues(const K& key)
    {
        auto it = _keyToValueSet.find(key);
        if (it == _keyToValueSet.end()) {
            return {nullptr, false};
        } else {
            return {&it->second, true};
        }
    }

    std::pair<std::unordered_set<V>, bool> removeKey(const K& key)
    {
        auto it = _keyToValueSet.find(key);
        if (it == _keyToValueSet.end()) {
            return {{}, false};
        } else {
            auto res = std::move(it->second);
            _keyToValueSet.erase(it);
            return {std::move(res), true};
        }
    }

    bool removeValue(const K& key, const V& value)
    {
        auto it = _keyToValueSet.find(key);
        if (it == _keyToValueSet.end()) {
            return false;
        }
        auto vit = it->second.find(value);
        if (vit == it->second.end()) {
            return false;
        }
        it->second.erase(vit);
        if (it->second.empty()) {
            _keyToValueSet.erase(it);
        }
        return true;
    }
};

template <typename LeftK, typename RightK>
struct ManyToManyDict {
    KeyValueDictSet<LeftK, RightK> _leftKeyToRightKey;
    KeyValueDictSet<RightK, LeftK> _rightKeyToLeftKey;

    const KeyValueDictSet<LeftK, RightK>& leftKeys() { return _leftKeyToRightKey; }
    const KeyValueDictSet<RightK, LeftK>& rightKeys() { return _rightKeyToLeftKey; }

    void add(LeftK leftKey, RightK rightKey)
    {
        _leftKeyToRightKey.add(leftKey, rightKey);
        _rightKeyToLeftKey.add(std::move(rightKey), std::move(leftKey));
        return;
    }

    bool remove(const LeftK& leftKey, const RightK& rightKey)
    {
        return _leftKeyToRightKey.removeValue(leftKey, rightKey) && _rightKeyToLeftKey.removeValue(rightKey, leftKey);
    }

    bool hasLeftKey(const LeftK& leftKey) { return _leftKeyToRightKey.contains(leftKey); }
    bool hasRightKey(const RightK& rightKey) { return _rightKeyToLeftKey.contains(rightKey); }

    bool hasKeyPair(const LeftK& leftKey, const RightK& rightKey)
    {
        const auto [leftKeyVals, leftKeyValid] = _leftKeyToRightKey.getValues(leftKey);
        if (!leftKeyValid || !leftKeyVals->contains(rightKey)) {
            return false;
        }

        const auto [rightKeyVals, rightKeyValid] = _rightKeyToLeftKey.getValues(rightKey);
        if (!rightKeyValid || !rightKeyVals->contains(leftKey)) {
            return false;
        }

        return hasLeftKey(leftKey) && hasRightKey(rightKey);
    }

    const auto& leftKeyItems() { return _leftKeyToRightKey.items(); }
    const auto& rightKeyItems() { return _rightKeyToLeftKey.items(); }

    std::pair<std::unordered_set<LeftK>*, bool> getLeftItems(const RightK& rightKey)
    {
        return _rightKeyToLeftKey.getValues(rightKey);
    }

    std::pair<std::unordered_set<RightK>*, bool> getRightItems(const LeftK& leftKey)
    {
        return _leftKeyToRightKey.getValues(leftKey);
    }

    std::pair<std::unordered_set<RightK>, bool> removeLeftKey(const LeftK& leftKey)
    {
        auto res = _leftKeyToRightKey.removeKey(leftKey);
        for (const auto& key: res.first) {
            _rightKeyToLeftKey.removeValue(key, leftKey);
        }
        return res;
    }

    std::pair<std::unordered_set<LeftK>, bool> removeRightKey(const RightK& rightKey)
    {
        auto res = _rightKeyToLeftKey.removeKey(rightKey);
        for (const auto& key: res.first) {
            _leftKeyToRightKey.removeValue(key, rightKey);
        }
        return res;
    }
};

}  // namespace utility
}  // namespace scaler
