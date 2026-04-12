# protocol_objs ======================================================================================================
# compile all protocol schema capnp files
set(CAPNPC_SRC_PREFIX "${PROJECT_SOURCE_DIR}/src/protocol" CACHE STRING "" FORCE)
set(CAPNPC_OUTPUT_DIR "${PROJECT_SOURCE_DIR}/src/protocol" CACHE STRING "" FORCE)
file(MAKE_DIRECTORY "${CAPNPC_OUTPUT_DIR}")

capnp_generate_cpp(
    COMMON_CAPNP_SRCS COMMON_CAPNP_HEADERS "${PROJECT_SOURCE_DIR}/src/protocol/common.capnp"
)
capnp_generate_cpp(
    STATUS_CAPNP_SRCS STATUS_CAPNP_HEADERS "${PROJECT_SOURCE_DIR}/src/protocol/status.capnp"
)
capnp_generate_cpp(
    MESSAGE_CAPNP_SRCS MESSAGE_CAPNP_HEADERS "${PROJECT_SOURCE_DIR}/src/protocol/message.capnp"
)
capnp_generate_cpp(
    OBJECT_STORAGE_CAPNP_SRCS OBJECT_STORAGE_CAPNP_HEADERS
    "${PROJECT_SOURCE_DIR}/src/protocol/object_storage.capnp"
)

add_library(protocol_objs OBJECT
    ${COMMON_CAPNP_SRCS}
    ${COMMON_CAPNP_HEADERS}
    ${STATUS_CAPNP_SRCS}
    ${STATUS_CAPNP_HEADERS}
    ${MESSAGE_CAPNP_SRCS}
    ${MESSAGE_CAPNP_HEADERS}
    ${OBJECT_STORAGE_CAPNP_SRCS}
    ${OBJECT_STORAGE_CAPNP_HEADERS}
)

target_include_directories(protocol_objs PUBLIC "${PROJECT_SOURCE_DIR}/src")

# py_capnp python module ==============================================================================================
scaler_add_python_module(
    TARGET py_capnp
    MODULE_NAME capnp
    INSTALL_DEST scaler/protocol/python
    SOURCES
        pymod/bootstrap.cpp
        pymod/capnp.cpp
        pymod/module_state.cpp
        pymod/schema_registry.cpp
        pymod/serialize.cpp
        pymod/utility.cpp
    LINK_LIBRARIES
        protocol_objs
        CapnProto::capnp
        CapnProto::capnpc
        CapnProto::kj
)
