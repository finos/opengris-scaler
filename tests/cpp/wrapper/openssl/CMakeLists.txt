add_test_executable(test_wrapper_openssl test_openssl.cpp)

target_link_libraries(test_wrapper_openssl PRIVATE
    scaler_wrapper_openssl
    scaler_wrapper_uv
)

# Copy sample certificates into the build directory
configure_file(sample_cert.pem ${CMAKE_CURRENT_BINARY_DIR}/sample_cert.pem COPYONLY)
configure_file(sample_private_key.pem ${CMAKE_CURRENT_BINARY_DIR}/sample_private_key.pem COPYONLY)

set_tests_properties(test_wrapper_openssl PROPERTIES WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
