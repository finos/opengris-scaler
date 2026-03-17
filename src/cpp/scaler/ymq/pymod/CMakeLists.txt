set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/src/scaler/io/ymq)

find_package(Python3 COMPONENTS Development.Module REQUIRED)

add_library(py_ymq SHARED
    address.h
    binder_socket.h
    bytes.h
    connector_socket.h
    exception.h
    io_context.h
    message.h
    ymq.h
    ymq.cpp
    $<TARGET_OBJECTS:ymq_objs>
)

target_include_directories(py_ymq PRIVATE
    ${PROJECT_SOURCE_DIR}/src/cpp
)

target_link_libraries(py_ymq PRIVATE
    scaler_wrapper_uv
    Python3::Module
)

set_target_properties(py_ymq PROPERTIES
    PREFIX ""
    OUTPUT_NAME "_ymq"
    LINKER_LANGUAGE CXX
)

if(WIN32)
    set_target_properties(py_ymq PROPERTIES
        SUFFIX ".pyd"
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/src/scaler/io/ymq
        RUNTIME_OUTPUT_DIRECTORY_RELEASE ${CMAKE_BINARY_DIR}/src/scaler/io/ymq
        RUNTIME_OUTPUT_DIRECTORY_DEBUG ${CMAKE_BINARY_DIR}/src/scaler/io/ymq
        RUNTIME_OUTPUT_DIRECTORY_RELWITHDEBINFO ${CMAKE_BINARY_DIR}/src/scaler/io/ymq
        RUNTIME_OUTPUT_DIRECTORY_MINSIZEREL ${CMAKE_BINARY_DIR}/src/scaler/io/ymq
    )
endif()

install(
    TARGETS py_ymq
    RUNTIME DESTINATION scaler/io/ymq
    LIBRARY DESTINATION scaler/io/ymq
    ARCHIVE DESTINATION scaler/io/ymq
)
