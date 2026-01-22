add_test_executable(test_wrapper_uv test_uv.cpp)

if(WIN32)
    target_sources(test_wrapper_uv PRIVATE test_uv_windows.cpp)
    target_sources(test_wrapper_uv PRIVATE test_pipe_windows.cpp)
else()
    target_sources(test_wrapper_uv PRIVATE test_uv_unix.cpp)
    target_sources(test_wrapper_uv PRIVATE test_pipe_unix.cpp)
endif()

target_sources(test_wrapper_uv PRIVATE test_tcp.cpp)
target_sources(test_wrapper_uv PRIVATE test_pipe.cpp)
