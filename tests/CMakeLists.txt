include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/155b337c938a2953e5675f9dc18c99f05f4c85d0.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
set(BUILD_GTEST ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

find_package(Python3 COMPONENTS Development REQUIRED)

# This function compiles, links, and adds a C++ test executable using Google Test.
# It is shared by all test subdirectories.
function(add_test_executable test_name source_file)
    add_executable(${test_name} ${source_file})

    target_include_directories(${test_name} PRIVATE ${CMAKE_BINARY_DIR})

    target_link_libraries(${test_name} PRIVATE
        $<$<NOT:$<PLATFORM_ID:Windows>>:object_storage_server_objs>
        scaler_wrapper_uv
        uv_ymq_objs
        ymq_objs
        CapnProto::capnp
        CapnProto::kj
        GTest::gtest_main
        Python3::Python
    )

    add_test(NAME ${test_name} COMMAND ${test_name})

    # Post build: We need to copy if dll is not present, otherwise the test program won't find gtest.dll
    # See https://stackoverflow.com/questions/69978314/cmake-with-gtest-on-windows-build-starts-test-but-shared-libs-cannot-be-found
    if (WIN32)
        add_custom_command(TARGET ${test_name} POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy_if_different
                $<TARGET_RUNTIME_DLLS:${test_name}>
                $<TARGET_FILE_DIR:${test_name}>
            COMMAND_EXPAND_LISTS
        )
    endif()
endfunction()

# Add the directory for the C++ tests.
add_subdirectory(cpp)
