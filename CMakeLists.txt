cmake_minimum_required(VERSION 3.28 FATAL_ERROR)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# set(CMAKE_CXX_SCAN_FOR_MODULES ON)
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)

if(NOT SKBUILD_PROJECT_NAME)
  set(SKBUILD_PROJECT_NAME "scaler")
endif()

if(NOT SKBUILD_PROJECT_VERSION)
  set(SKBUILD_PROJECT_VERSION "1.9.4")
endif()

project(
  ${SKBUILD_PROJECT_NAME}
  LANGUAGES C CXX
  VERSION ${SKBUILD_PROJECT_VERSION}
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


add_subdirectory(scaler)
