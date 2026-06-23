set(OPENSSL_USE_STATIC_LIBS ON)
find_package(OpenSSL CONFIG REQUIRED)

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

# Statically linked OpenSSL on Windows requires crypt32.
if(WIN32)
    target_link_libraries(scaler_wrapper_openssl PUBLIC crypt32)
endif()
