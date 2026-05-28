find_package(OpenSSL QUIET)

if(NOT OpenSSL_FOUND)
    message(STATUS "OpenSSL not found -- skipping scaler_wrapper_openssl (not needed for py_capnp wheel)")
    return()
endif()

add_library(scaler_wrapper_openssl STATIC
    secure_server.h
    secure_server.cpp
    secure_socket.h
    secure_socket.cpp
    ssl_context.h
    ssl_context.cpp
)

target_link_libraries(scaler_wrapper_openssl
    PUBLIC
        scaler_wrapper_uv
        OpenSSL::SSL
        OpenSSL::Crypto
)
