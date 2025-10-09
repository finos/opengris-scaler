# capnp_objs ===========================================================================================================
# compile object storage schema capnp
set(CAPNPC_SRC_PREFIX "${PROJECT_SOURCE_DIR}/scaler/protocol/capnp" CACHE STRING "" FORCE)
set(CAPNPC_OUTPUT_DIR "${PROJECT_SOURCE_DIR}/scaler/protocol/capnp" CACHE STRING "" FORCE)
file(MAKE_DIRECTORY "${CAPNPC_OUTPUT_DIR}")
capnp_generate_cpp(OBJECT_STORAGE_CAPNP_SRCS OBJECT_STORAGE_CAPNP_HEADERS "../protocol/capnp/object_storage.capnp")

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

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/scaler/object_storage)

# object_storage_server python =========================================================================================
add_library(py_object_storage_server MODULE
    pymod_object_storage_server.cpp
)

set_target_properties(py_object_storage_server PROPERTIES
    PREFIX ""
    OUTPUT_NAME "object_storage_server"
)

target_link_libraries(py_object_storage_server PRIVATE
    $<$<CXX_COMPILER_ID:GNU,Clang>:-Wl,-Bstatic>
    capnp_objs
    ymq_objs
    object_storage_server_objs
    $<$<CXX_COMPILER_ID:GNU,Clang>:-Wl,-Bdynamic>
    CapnProto::capnp
    CapnProto::kj
    Python3::Module
)

install(
    TARGETS py_object_storage_server
    LIBRARY DESTINATION scaler/object_storage/
    COMPONENT object_storage_server
)
