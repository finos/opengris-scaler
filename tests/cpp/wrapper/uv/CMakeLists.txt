add_test_executable(test_wrapper_uv test_uv.cpp)

if(WIN32)
    target_sources(test_wrapper_uv PRIVATE test_uv_windows.cpp)
else()
    target_sources(test_wrapper_uv PRIVATE test_uv_unix.cpp)
endif()

target_sources(test_wrapper_uv PRIVATE test_tcp.cpp)
