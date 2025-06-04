add_library(cc_ymq SHARED)

target_sources(cc_ymq PRIVATE
  coro_context.h
  coro_context.cpp
)

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/scaler/io/ymq)

install(TARGETS cc_ymq
        LIBRARY DESTINATION scaler/io)

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
        LIBRARY DESTINATION scaler/io/ymq)
