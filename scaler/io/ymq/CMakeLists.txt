
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

install(TARGETS ymq
        LIBRARY DESTINATION ${CMAKE_SOURCE_DIR}/scaler/lib)

add_subdirectory(examples)
