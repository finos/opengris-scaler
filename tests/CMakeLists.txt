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
        $<$<CXX_COMPILER_ID:GNU,Clang>:-Wl,-Bstatic>
        object_storage_server_objs
        ymq_objs
        $<$<CXX_COMPILER_ID:GNU,Clang>:-Wl,-Bdynamic>
        CapnProto::capnp
        CapnProto::kj
        GTest::gtest_main
        Python3::Python
    )

    add_test(NAME ${test_name} COMMAND ${test_name})
endfunction()

if(LINUX OR APPLE)
  # Add the directory for the C++ tests.
  add_subdirectory(cpp)

  # Add the new directory for io tests.
  add_subdirectory(io/ymq)
endif()
