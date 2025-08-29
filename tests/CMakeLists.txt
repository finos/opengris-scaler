# This function compiles, links, and adds a C++ test executable using Google Test.
# It is shared by all test subdirectories.
function(add_test_executable test_name source_file)
    add_executable(${test_name} ${source_file})

    target_include_directories(${test_name} PRIVATE ${CMAKE_BINARY_DIR})

    target_link_libraries(
        ${test_name}
        PRIVATE GTest::gtest_main
        PRIVATE cc_object_storage_server
    )

    add_test(NAME ${test_name} COMMAND ${test_name})
endfunction()


if(LINUX OR APPLE)
  # This directory fetches Google Test, so it must be included first.
  add_subdirectory(object_storage)

  # Add the new directory for io tests.
  add_subdirectory(io/ymq)
endif()