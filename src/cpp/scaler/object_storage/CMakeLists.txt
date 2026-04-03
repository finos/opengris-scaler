# capnp_objs ===========================================================================================================
# compile object storage schema capnp
set(CAPNPC_SRC_PREFIX "${PROJECT_SOURCE_DIR}/src/scaler/protocol/capnp" CACHE STRING "" FORCE)
set(CAPNPC_OUTPUT_DIR "${PROJECT_SOURCE_DIR}/src/scaler/protocol/capnp" CACHE STRING "" FORCE)
file(MAKE_DIRECTORY "${CAPNPC_OUTPUT_DIR}")
capnp_generate_cpp(OBJECT_STORAGE_CAPNP_SRCS OBJECT_STORAGE_CAPNP_HEADERS "${PROJECT_SOURCE_DIR}/src/scaler/protocol/capnp/object_storage.capnp")

add_library(capnp_objs OBJECT
    ${OBJECT_STORAGE_CAPNP_SRCS}
    ${OBJECT_STORAGE_CAPNP_HEADERS}
)

# object_storage_server_objs ===========================================================================================
add_library(object_storage_server_objs OBJECT
    io_helper.cpp
    message.cpp
    object_storage_server.cpp
    object_manager.cpp
)


# object_storage_server python =========================================================================================
scaler_add_python_module(
    TARGET py_object_storage_server
    MODULE_NAME object_storage_server
    INSTALL_DEST scaler/object_storage
    SOURCES pymod_object_storage_server.cpp
    LINK_LIBRARIES
        capnp_objs
        ymq_objs
        object_storage_server_objs
        CapnProto::capnp
        CapnProto::kj
)
