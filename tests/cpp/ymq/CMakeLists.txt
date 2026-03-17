add_test_executable(test_ymq test_ymq.cpp)

target_sources(test_ymq PRIVATE
    test_binder_socket.cpp
    test_connector_socket.cpp
    test_sockets.cpp
    test_sync.cpp
)

add_subdirectory(internal)

if(LINUX OR WIN32)
	target_sources(test_ymq PRIVATE
		test_mitm.cpp)
else()
	message(WARNING "YMQ MITM tests are not supported on this platform")
endif()

target_sources(test_ymq PRIVATE
	common/testing.h
	common/testing.cpp
	common/utils.h
	common/utils.cpp

	net/socket.h
	net/socket_utils.h
	net/tcp_socket.h
	net/uds_socket.h

	pipe/pipe.h
	pipe/pipe_utils.h
	pipe/pipe_reader.h
	pipe/pipe_writer.h
)
if(LINUX OR APPLE)
	target_sources(test_ymq PRIVATE
		common/utils_linux.cpp
		common/testing_linux.cpp

		net/socket_utils_linux.cpp
		net/tcp_socket_linux.cpp
		net/uds_socket.cpp

		pipe/pipe_utils_linux.cpp
		pipe/pipe_reader_linux.cpp
		pipe/pipe_writer_linux.cpp)
elseif(WIN32)
	target_sources(test_ymq PRIVATE
		common/testing_windows.cpp
		common/utils_windows.cpp

		net/socket_utils_windows.cpp
		net/tcp_socket_windows.cpp

		pipe/pipe_utils_windows.cpp
		pipe/pipe_reader_windows.cpp
		pipe/pipe_writer_windows.cpp)
endif()
