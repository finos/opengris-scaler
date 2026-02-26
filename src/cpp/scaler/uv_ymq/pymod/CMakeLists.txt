set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/src/scaler/io/uv_ymq)

find_package(Python3 COMPONENTS Development.Module REQUIRED)

add_library(py_uv_ymq SHARED
    address.h
    io_context.h
    uv_ymq.h
    uv_ymq.cpp
    $<TARGET_OBJECTS:uv_ymq_objs>
)

target_include_directories(py_uv_ymq PRIVATE
    ${PROJECT_SOURCE_DIR}/src/cpp
)

target_link_libraries(py_uv_ymq PRIVATE
    Python3::Module
    scaler_wrapper_uv
)

set_target_properties(py_uv_ymq PROPERTIES
    PREFIX ""
    OUTPUT_NAME "_uv_ymq"
    LINKER_LANGUAGE CXX
)

if(WIN32)
    set_target_properties(py_uv_ymq PROPERTIES
        SUFFIX ".pyd"
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/src/scaler/io/uv_ymq
        RUNTIME_OUTPUT_DIRECTORY_RELEASE ${CMAKE_BINARY_DIR}/src/scaler/io/uv_ymq
        RUNTIME_OUTPUT_DIRECTORY_DEBUG ${CMAKE_BINARY_DIR}/src/scaler/io/uv_ymq
        RUNTIME_OUTPUT_DIRECTORY_RELWITHDEBINFO ${CMAKE_BINARY_DIR}/src/scaler/io/uv_ymq
        RUNTIME_OUTPUT_DIRECTORY_MINSIZEREL ${CMAKE_BINARY_DIR}/src/scaler/io/uv_ymq
    )
endif()

install(
    TARGETS py_uv_ymq
    RUNTIME DESTINATION scaler/io/uv_ymq
    LIBRARY DESTINATION scaler/io/uv_ymq
    ARCHIVE DESTINATION scaler/io/uv_ymq
)
