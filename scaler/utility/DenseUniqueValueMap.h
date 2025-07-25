#pragma once

#include <cstdlib>
#include <map>
#include <vector>

struct DenseUniqueValueMap {
    using key_type      = int;     // IRL we store client here
    using value_type    = size_t;  // IRL we store taskId here
    using iterator_type = std::map<key_type, size_t>::iterator;
    std::map<value_type, iterator_type> valueToKeyIter;
    // Bunch of keys(clients), and number of tasks belongs to it
    std::map<key_type, size_t> keyToRefCount;

    key_type get_key(value_type value) {
        if (!valueToKeyIter.contains(value)) {
            // TODO: handle error more gracefully
            exit(1);
        }
        auto keyIter = valueToKeyIter[value];
        return keyIter->first;
    }

    void add(key_type key, value_type value) {
        auto it = valueToKeyIter.find(value);
        if (it != valueToKeyIter.end()) {
            if (it->first != key) {
                // TODO: handle error more gracefully
                exit(1);
            } else {
                return;
            }
        }

        auto [kIter, _] = keyToRefCount.insert_or_assign(key, ++keyToRefCount[key]);
        it->second      = kIter;
    }

    key_type removeValue(value_type value) {
        auto it = valueToKeyIter.find(value);
        if (it == valueToKeyIter.end()) {
            // TODO: handle error more gracefully
            exit(1);
        }
        auto keysMapIter = it->second;
        valueToKeyIter.erase(it);
        auto key = keysMapIter->first;
        if (keysMapIter->second == 1) {
            keyToRefCount.erase(keysMapIter);
        }
        return key;
    }

    auto& getKeysRef() { return keyToRefCount; }
};
