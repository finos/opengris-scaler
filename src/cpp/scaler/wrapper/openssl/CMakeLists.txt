find_package(OpenSSL REQUIRED)

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
