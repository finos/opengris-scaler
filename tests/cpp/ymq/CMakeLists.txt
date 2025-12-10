add_test_executable(test_ymq SOURCES test_ymq.cpp)

target_sources(test_ymq PRIVATE
	common/testing.h
	common/utils.h
	common/utils.cpp

	net/socket.h
)

if(LINUX OR APPLE)
	target_sources(test_ymq PRIVATE
		net/socket_linux.cpp)
elseif(WIN32)
	target_sources(test_ymq PRIVATE
		net/socket_windows.cpp)
endif()
