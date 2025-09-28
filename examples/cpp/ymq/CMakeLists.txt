
add_executable(echo_server echo_server.cpp)
target_link_libraries(echo_server ymq_objs)

add_executable(echo_client echo_client.cpp)
target_link_libraries(echo_client ymq_objs)

add_executable(timestamp timestamp.cpp)
target_link_libraries(timestamp ymq_objs)

add_executable(remove_iosocket remove_iosocket.cpp)
target_link_libraries(remove_iosocket ymq_objs)

add_executable(sub_client sub_client.cpp)
target_link_libraries(sub_client ymq_objs)

add_executable(pub_server pub_server.cpp)
target_link_libraries(pub_server ymq_objs)

add_executable(automated_echo_client automated_echo_client.cpp)
target_link_libraries(automated_echo_client ymq_objs)

add_executable(pingpong pingpong.cpp)
target_link_libraries(pingpong ymq_objs)
