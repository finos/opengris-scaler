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

add_test_executable(test_object_manager test_object_manager.cpp)
add_test_executable(test_object_storage_server test_object_storage_server.cpp)