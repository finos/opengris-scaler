add_test_executable(test_uv_ymq test_uv_ymq.cpp)

target_sources(test_uv_ymq PRIVATE
    test_connect_client.cpp
    test_accept_server.cpp
)
