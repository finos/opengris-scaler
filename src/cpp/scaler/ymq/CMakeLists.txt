add_library(ymq_objs OBJECT
    bytes.h
    common.h
    configuration.h

    epoll_context.h
    epoll_context.cpp

    iocp_context.h
    iocp_context.cpp

    event_loop.h

    event_loop_thread.h
    event_loop_thread.cpp

    event_manager.h

    message_connection.h
    message_connection.cpp

    third_party/concurrentqueue.h
    interruptive_concurrent_queue.h

    typedefs.h

    io_context.h
    io_context.cpp

    io_socket.h
    io_socket.cpp

    stream_server.h
    stream_server.cpp

    stream_client.h
    stream_client.cpp

    tcp_operations.h

    timestamp.h

    timed_queue.h

    network_utils.h

    simple_interface.h
    simple_interface.cpp

    internal/defs.h

    internal/raw_stream_connection_handle.h

    internal/raw_stream_server_handle.h

    internal/raw_stream_client_handle.h
)

# System dependent file for YMQ internal
if(LINUX)
    target_sources(ymq_objs PRIVATE
        internal/raw_stream_connection_handle_linux.cpp
        internal/raw_stream_server_handle_linux.cpp
        internal/raw_stream_client_handle_linux.cpp
    )
elseif(WIN32)
    target_sources(ymq_objs PRIVATE
        internal/raw_stream_connection_handle_windows.cpp
        internal/raw_stream_server_handle_windows.cpp
        internal/raw_stream_client_handle_windows.cpp
    )
endif()

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/src/scaler/ymq)

if(LINUX)
    # ymq python =======================================================================================================

    add_library(py_ymq SHARED
        pymod_ymq/bytes.h
        pymod_ymq/exception.h
        pymod_ymq/gil.h
        pymod_ymq/message.h
        pymod_ymq/io_context.h
        pymod_ymq/io_socket.h
        pymod_ymq/ymq.h
        pymod_ymq/ymq.cpp
    )

    target_include_directories(py_ymq PRIVATE
        ${PROJECT_SOURCE_DIR}/src/cpp
    )

    target_link_libraries(py_ymq PRIVATE
        $<$<CXX_COMPILER_ID:GNU,Clang>:-Wl,-Bstatic> ymq_objs
        $<$<CXX_COMPILER_ID:GNU,Clang>:-Wl,-Bdynamic>
        Python3::Module
    )

    set_target_properties(py_ymq PROPERTIES
        PREFIX ""
        OUTPUT_NAME "_ymq"
        LINKER_LANGUAGE CXX
    )

    install(
        TARGETS py_ymq
        LIBRARY DESTINATION scaler/io/ymq
    )
endif()

if(WIN32)
    # ymq python =======================================================================================================

    target_link_libraries(ymq_objs PRIVATE "ws2_32")
    target_compile_definitions(ymq_objs PRIVATE _WINSOCKAPI_=) # Yes, trailing equal to guarantee empty def
endif()
