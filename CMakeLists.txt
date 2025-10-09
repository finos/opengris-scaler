cmake_minimum_required(VERSION 3.28 FATAL_ERROR)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)
set(CMAKE_VERBOSE_MAKEFILE OFF)
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
if(WIN32)
    # Windows
    message(STATUS "Windows")
    
    set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS on)
    add_compile_definitions(WIN32_LEAN_AND_MEAN)
    add_compile_options("/EHsc")
   
elseif(APPLE)
    if(CMAKE_OSX_ARCHITECTURES MATCHES "arm64" OR CMAKE_SYSTEM_PROCESSOR MATCHES "arm64|aarch64")
        # Mac Arm
        message(STATUS "macOS on ARM64")

        add_compile_options(-Wall -Wextra)
        add_link_options(-Wl,-rpath,@loader_path)
    else()
        # Mac Intel
        message(STATUS "macOS on x86_64")

        add_compile_options(-Wall -Wextra)
        add_link_options(-Wl,-rpath,@loader_path)
    endif()

elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        # Linux GCC
        message(STATUS "Linux GCC ${CMAKE_CXX_COMPILER_VERSION}")
    
        add_compile_options(-Wall -Wsubobject-linkage)
        add_link_options(-Wl,-rpath,$ORIGIN)

    elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang") # matches Clang and AppleClang
        #Linux Clang
        message(STATUS "Linux Clang ${CMAKE_CXX_COMPILER_VERSION}")

        add_compile_options(-Wall -stdlib=libc++)
        add_link_options(-stdlib=libc++)

        # needed for jthread in libc++
        add_compile_options(-fexperimental-library)
        add_link_options(-fexperimental-library)

    else()
        message(WARNING "Unknown compiler on Linux")
    endif()
else()
    message(WARNING "Unknown platform: ${CMAKE_SYSTEM_NAME}")
endif()


find_package(CapnProto CONFIG REQUIRED)
get_target_property(CAPNP_INCLUDE_DIRS CapnProto::capnp INTERFACE_INCLUDE_DIRECTORIES)
message(STATUS "Found Capnp includes in ${CAPNP_INCLUDE_DIRS}")

execute_process(
  COMMAND python3-config --prefix
  OUTPUT_VARIABLE PYTHON_PREFIX
  OUTPUT_STRIP_TRAILING_WHITESPACE
)
set(Python3_ROOT_DIR "${PYTHON_PREFIX}")

find_package(Python3 REQUIRED COMPONENTS Development.Module)
message(STATUS "Python version: ${Python3_VERSION}")
message(STATUS "Python include dirs: ${Python3_INCLUDE_DIRS}")
message(STATUS "Python ABI: ${Python3_SOABI}")

# Make LSP happy
include_directories(${CAPNP_INCLUDE_DIRS})
include_directories(${PROJECT_SOURCE_DIR})

add_subdirectory(scaler)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  add_subdirectory(tests)
  add_subdirectory(examples)
endif()
