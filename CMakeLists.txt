cmake_minimum_required(VERSION 3.28 FATAL_ERROR)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# set(CMAKE_CXX_SCAN_FOR_MODULES ON)
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)
set(BUILD_SHARED_LIBS ON CACHE BOOL "Build shared libraries")

if(NOT SKBUILD_PROJECT_NAME)
  set(SKBUILD_PROJECT_NAME "scaler")
endif()

# Path to the version file
set(VERSION_FILE "${CMAKE_CURRENT_SOURCE_DIR}/scaler/version.txt")

# Check if the version file exists
if(NOT EXISTS "${VERSION_FILE}")
    message(FATAL_ERROR "Version file not found: ${VERSION_FILE}")
endif()

# Read version from file
file(READ "${VERSION_FILE}" VERSION_CONTENTS)

# Strip whitespace from the version string
string(STRIP "${VERSION_CONTENTS}" PROJECT_VERSION)

project(
  ${SKBUILD_PROJECT_NAME}
  LANGUAGES C CXX
  VERSION ${PROJECT_VERSION}
)

configure_file(
	${PROJECT_SOURCE_DIR}/version.h.in
	${PROJECT_BINARY_DIR}/version.h
)

enable_testing()

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release")
  message(STATUS "${PROJECT_NAME} defaulting to Release build")
elseif(NOT CMAKE_BUILD_TYPE STREQUAL "Debug" AND NOT CMAKE_BUILD_TYPE STREQUAL "Release")
  message(FATAL_ERROR "${PROJECT_NAME} invalid CMAKE_BUILD_TYPE: '${CMAKE_BUILD_TYPE}'. Must be 'Debug' or 'Release'.")
else()
  message(STATUS "${PROJECT_NAME} ${CMAKE_BUILD_TYPE} build")
endif()

# Compiler configuration
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(STATUS "Using Clang: adding -stdlib=libc++")
    add_compile_options(-stdlib=libc++)
    add_link_options(-stdlib=libc++)

    # needed for jthread in libc++
    add_compile_options(-fexperimental-library)
    add_link_options(-fexperimental-library)
endif()

find_package(CapnProto CONFIG REQUIRED)
get_target_property(CAPNP_INCLUDE_DIRS CapnProto::capnp INTERFACE_INCLUDE_DIRECTORIES)
message(STATUS "Found Capnp in ${CAPNP_INCLUDE_DIRS}")
# Make LSP happy
include_directories(${CAPNP_INCLUDE_DIRS})
include_directories(${PROJECT_SOURCE_DIR})

add_subdirectory(scaler)

if(NOT SKBUILD_STATE)
  add_subdirectory(tests)
endif()
