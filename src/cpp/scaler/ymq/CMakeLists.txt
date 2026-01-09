# Modified CMakeLists.txt - Windows fixes so installer finds the built Python extension (.pyd)
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

    simple_interface.h
    simple_interface.cpp

    internal/network_utils.h

    internal/raw_stream_connection_handle.h

    internal/raw_stream_server_handle.h

    internal/raw_stream_client_handle.h

    internal/socket_address.h
)

# System dependent file for YMQ internal
if(LINUX)
    target_sources(ymq_objs PRIVATE
        internal/network_utils_linux.cpp
        internal/socket_address_linux.cpp
        internal/raw_stream_connection_handle_linux.cpp
        internal/raw_stream_server_handle_linux.cpp
        internal/raw_stream_client_handle_linux.cpp
    )
elseif(WIN32)
    target_sources(ymq_objs PRIVATE
        internal/network_utils_windows.cpp
        internal/socket_address_windows.cpp
        internal/raw_stream_connection_handle_windows.cpp
        internal/raw_stream_server_handle_windows.cpp
        internal/raw_stream_client_handle_windows.cpp
    )
endif()

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/src/scaler/ymq)

# Ensure Python development module support is found
find_package(Python3 COMPONENTS Development.Module REQUIRED)

add_library(py_ymq SHARED
    pymod_ymq/bytes.h
    pymod_ymq/exception.h
    pymod_ymq/message.h
    pymod_ymq/io_context.h
    pymod_ymq/io_socket.h
    pymod_ymq/ymq.h
    pymod_ymq/ymq.cpp
    $<TARGET_OBJECTS:ymq_objs>
)

target_include_directories(py_ymq PRIVATE
    ${PROJECT_SOURCE_DIR}/src/cpp
    ${PROJECT_SOURCE_DIR}/src/cpp/pymod_ymq
)

target_link_libraries(py_ymq PRIVATE
    Python3::Module
)

if(WIN32)
    # Link Winsock for Windows
    target_link_libraries(py_ymq PRIVATE ws2_32)
endif()

# Keep compatibility with name expected by Python imports
set_target_properties(py_ymq PROPERTIES
    PREFIX ""
    OUTPUT_NAME "_ymq"
    LINKER_LANGUAGE CXX
)

# --- Windows-specific fixes for installer lookup ---
# On multi-config generators (Visual Studio) the runtime output gets placed into a subdir like "Release".
# The generated install script will look for the configured runtime output (e.g. .../Release/_ymq.*).
# CMake on Windows by default will name a shared module as .dll; Python expects .pyd for extensions.
# Make the target produce a .pyd and put it into the expected runtime output directory for all configs.
if(WIN32)
    # Ensure the built Python extension on Windows has a .pyd suffix so the install target looks for _ymq.pyd
    set_target_properties(py_ymq PROPERTIES
        SUFFIX ".pyd"
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/src/scaler/ymq
        # Set config-specific outputs as well so Visual Studio places them in the same folder (no Release/ subdir)
        RUNTIME_OUTPUT_DIRECTORY_RELEASE ${CMAKE_BINARY_DIR}/src/scaler/ymq
        RUNTIME_OUTPUT_DIRECTORY_DEBUG ${CMAKE_BINARY_DIR}/src/scaler/ymq
        RUNTIME_OUTPUT_DIRECTORY_RELWITHDEBINFO ${CMAKE_BINARY_DIR}/src/scaler/ymq
        RUNTIME_OUTPUT_DIRECTORY_MINSIZEREL ${CMAKE_BINARY_DIR}/src/scaler/ymq
    )
endif()

# Install the extension; include RUNTIME so .pyd on Windows is handled
install(
    TARGETS py_ymq
    RUNTIME DESTINATION scaler/io/ymq
    LIBRARY DESTINATION scaler/io/ymq
    ARCHIVE DESTINATION scaler/io/ymq
)

# Keep ymq_objs linking/compile defs for Windows
if(WIN32)
    target_link_libraries(ymq_objs PRIVATE ws2_32)
    target_compile_definitions(ymq_objs PRIVATE _WINSOCKAPI_=) # trailing '=' to guarantee empty def
endif()
