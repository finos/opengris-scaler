
add_executable(echo_server echo_server.cpp)
add_executable(timestamp timestamp.cpp)
add_executable(timed_queue timed_queue.cpp)

target_link_libraries(echo_server cc_ymq)
target_link_libraries(timestamp cc_ymq)
target_link_libraries(timed_queue cc_ymq)
