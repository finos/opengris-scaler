add_test_executable(test_ymq test_ymq.cpp)

target_sources(test_ymq PRIVATE
	common/testing.h
	common/utils.h
	common/utils.cpp

	net/socket.h

	pipe/pipe.h
	pipe/pipe_utils.h
	pipe/pipe_reader.h
	pipe/pipe_writer.h
)

if(LINUX OR APPLE)
	target_sources(test_ymq PRIVATE
		pipe/pipe_utils_linux.cpp
		pipe/pipe_reader_linux.cpp
		pipe/pipe_writer_linux.cpp
		net/socket_linux.cpp)
elseif(WIN32)
	target_sources(test_ymq PRIVATE
		pipe/pipe_utils_windows.cpp
		pipe/pipe_reader_windows.cpp
		pipe/pipe_writer_windows.cpp
		net/socket_windows.cpp)
endif()
