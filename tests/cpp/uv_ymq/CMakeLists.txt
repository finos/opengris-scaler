add_test_executable(test_uv_ymq test_uv_ymq.cpp)

target_sources(test_uv_ymq PRIVATE
    test_binder_socket.cpp
    test_connector_socket.cpp
    test_sync.cpp
)

add_subdirectory(internal)
