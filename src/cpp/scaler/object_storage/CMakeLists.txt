# object_storage_server_objs ===========================================================================================
add_library(object_storage_server_objs OBJECT
    io_helper.cpp
    message.cpp
    object_storage_server.cpp
    object_manager.cpp
)

target_link_libraries(object_storage_server_objs PUBLIC protocol_objs)

# object_storage_server python =========================================================================================
scaler_add_python_module(
    TARGET py_object_storage_server
    MODULE_NAME object_storage_server
    INSTALL_DEST scaler/object_storage
    SOURCES pymod_object_storage_server.cpp
    LINK_LIBRARIES
        protocol_objs
        ymq_objs
        object_storage_server_objs
        CapnProto::capnp
        CapnProto::kj
)
