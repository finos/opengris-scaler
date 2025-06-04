add_library(cc_ymq SHARED)

add_library(ymq SHARED)

target_sources(ymq PRIVATE
  io_context.h
  io_context.cpp

  event_loop.h

  event_loop_thread.h
  event_loop_thread.cpp

  io_socket.h
  io_socket.cpp

  configuration.hpp
)

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/scaler/io/ymq)

install(TARGETS cc_ymq
        LIBRARY DESTINATION scaler/io/ymq)

find_package(Python COMPONENTS Development)

add_library(ymq SHARED)

set_target_properties(ymq PROPERTIES  PREFIX "")

target_sources(ymq PRIVATE pymod_ymq/ymq.cpp)
target_include_directories(ymq PRIVATE ${Python_INCLUDE_DIRS})
target_link_libraries(ymq PRIVATE cc_ymq
                          PRIVATE ${Python_LIBRARIES}
)

target_link_options(ymq PRIVATE "-Wl,-rpath,$ORIGIN")

install(TARGETS ymq
        LIBRARY DESTINATION ${CMAKE_SOURCE_DIR}/scaler/lib)

add_subdirectory(examples)
