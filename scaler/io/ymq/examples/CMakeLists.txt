
add_executable(echo_server echo_server.cpp)
target_link_libraries(echo_server cc_ymq)

add_executable(echo_client echo_client.cpp)
target_link_libraries(echo_client cc_ymq)

add_executable(timestamp timestamp.cpp)
target_link_libraries(timestamp cc_ymq)

add_executable(remove_iosocket remove_iosocket.cpp)
target_link_libraries(remove_iosocket cc_ymq)

add_executable(sub_client sub_client.cpp)
target_link_libraries(sub_client cc_ymq)

add_executable(pub_server pub_server.cpp)
target_link_libraries(pub_server cc_ymq)

add_executable(automated_echo_client automated_echo_client.cpp)
target_link_libraries(automated_echo_client cc_ymq)

add_executable(pingpong pingpong.cpp)
target_link_libraries(pingpong cc_ymq)
