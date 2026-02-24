add_test_executable(test_uv_ymq test_uv_ymq.cpp)

target_sources(test_uv_ymq PRIVATE
    test_binder_socket.cpp
    test_connector_socket.cpp
    test_sockets.cpp
    test_sync.cpp
)

add_subdirectory(internal)

if(LINUX OR WIN32)
	target_sources(test_uv_ymq PRIVATE
		test_mitm.cpp)
else()
	message(WARNING "YMQ MITM tests are not supported on this platform")
endif()

target_sources(test_uv_ymq PRIVATE
	../ymq/common/testing.h
	../ymq/common/testing.cpp
	../ymq/common/utils.h
	../ymq/common/utils.cpp

	../ymq/net/socket.h
	../ymq/net/socket_utils.h
	../ymq/net/tcp_socket.h
	../ymq/net/uds_socket.h

	../ymq/pipe/pipe.h
	../ymq/pipe/pipe_utils.h
	../ymq/pipe/pipe_reader.h
	../ymq/pipe/pipe_writer.h
)
if(LINUX OR APPLE)
	target_sources(test_uv_ymq PRIVATE
		../ymq/common/utils_linux.cpp
		../ymq/common/testing_linux.cpp

		../ymq/net/socket_utils_linux.cpp
		../ymq/net/tcp_socket_linux.cpp
		../ymq/net/uds_socket.cpp

		../ymq/pipe/pipe_utils_linux.cpp
		../ymq/pipe/pipe_reader_linux.cpp
		../ymq/pipe/pipe_writer_linux.cpp)
elseif(WIN32)
	target_sources(test_uv_ymq PRIVATE
		../ymq/common/testing_windows.cpp
		../ymq/common/utils_windows.cpp

		../ymq/net/socket_utils_windows.cpp
		../ymq/net/tcp_socket_windows.cpp

		../ymq/pipe/pipe_utils_windows.cpp
		../ymq/pipe/pipe_reader_windows.cpp
		../ymq/pipe/pipe_writer_windows.cpp)
endif()
