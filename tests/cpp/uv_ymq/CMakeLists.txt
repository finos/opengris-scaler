add_test_executable(test_uv_ymq test_uv_ymq.cpp)

target_sources(test_uv_ymq PRIVATE
    test_connecting_client.cpp
    test_accepting_server.cpp
)
