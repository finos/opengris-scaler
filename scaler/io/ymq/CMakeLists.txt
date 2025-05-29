
add_library(ymq SHARED)

target_sources(ymq PRIVATE
  coro_context.h
  coro_context.cpp
)

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/scaler/io/ymq)

install(TARGETS ymq
        LIBRARY DESTINATION ${CMAKE_SOURCE_DIR}/scaler/lib)
