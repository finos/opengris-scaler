# one_to_many_dict python module
scaler_add_python_module(
    TARGET py_one_to_many_dict
    MODULE_NAME one_to_many_dict
    INSTALL_DEST scaler/utility
    SOURCES pymod/one_to_many_dict.cpp
)

# stable_priority_queue python module
scaler_add_python_module(
    TARGET py_stable_priority_queue
    MODULE_NAME stable_priority_queue
    INSTALL_DEST scaler/utility/queues
    SOURCES pymod/stable_priority_queue.cpp
)

# many_to_many_dict python module
scaler_add_python_module(
    TARGET py_many_to_many_dict
    MODULE_NAME many_to_many_dict
    INSTALL_DEST scaler/utility
    SOURCES pymod/many_to_many_dict.cpp
)

# indexed_queue python module
scaler_add_python_module(
    TARGET py_indexed_queue
    MODULE_NAME indexed_queue
    INSTALL_DEST scaler/utility/queues
    SOURCES pymod/indexed_queue.cpp
)
