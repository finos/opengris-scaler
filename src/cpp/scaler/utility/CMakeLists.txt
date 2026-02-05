# one_to_many_dict python ===========================================================================================
add_library(py_one_to_many_dict SHARED
    pymod/one_to_many_dict.cpp
)


target_include_directories(py_one_to_many_dict PRIVATE
    ${PROJECT_SOURCE_DIR}/src/cpp
)

target_link_libraries(py_one_to_many_dict PRIVATE
    Python3::Module
)

# stable_priority_queue python ===========================================================================================
add_library(py_stable_priority_queue SHARED
    pymod/stable_priority_queue.cpp
)


target_include_directories(py_stable_priority_queue PRIVATE
    ${PROJECT_SOURCE_DIR}/src/cpp
)

target_link_libraries(py_stable_priority_queue PRIVATE
    Python3::Module
)

# many_to_many_dict python ===========================================================================================
add_library(py_many_to_many_dict SHARED
    pymod/many_to_many_dict.cpp
)

target_include_directories(py_many_to_many_dict PRIVATE
    ${PROJECT_SOURCE_DIR}/src/cpp
)

target_link_libraries(py_many_to_many_dict PRIVATE
    Python3::Module
)

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/src/scaler/utility)

if(LINUX OR APPLE)
    set_target_properties(py_one_to_many_dict PROPERTIES
        PREFIX ""
        OUTPUT_NAME "one_to_many_dict"
        LINKER_LANGUAGE CXX
    )

    set_target_properties(py_stable_priority_queue PROPERTIES
        PREFIX ""
        OUTPUT_NAME "stable_priority_queue"
        LINKER_LANGUAGE CXX
    )

    set_target_properties(py_many_to_many_dict PROPERTIES
        PREFIX ""
        OUTPUT_NAME "many_to_many_dict"
        LINKER_LANGUAGE CXX
    )
elseif(WIN32)
    set_target_properties(py_one_to_many_dict PROPERTIES
        PREFIX ""
        OUTPUT_NAME "one_to_many_dict"
        SUFFIX ".pyd"
        LINKER_LANGUAGE CXX
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/src/scaler/utility
        RUNTIME_OUTPUT_DIRECTORY_RELEASE ${CMAKE_BINARY_DIR}/src/scaler/utility
        RUNTIME_OUTPUT_DIRECTORY_DEBUG ${CMAKE_BINARY_DIR}/src/scaler/utility
        RUNTIME_OUTPUT_DIRECTORY_RELWITHDEBINFO ${CMAKE_BINARY_DIR}/src/scaler/utility
        RUNTIME_OUTPUT_DIRECTORY_MINSIZEREL ${CMAKE_BINARY_DIR}/src/scaler/utility
    )

    set_target_properties(py_stable_priority_queue PROPERTIES
        PREFIX ""
        OUTPUT_NAME "stable_priority_queue"
        SUFFIX ".pyd"
        LINKER_LANGUAGE CXX
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/src/scaler/utility/queues
        RUNTIME_OUTPUT_DIRECTORY_RELEASE ${CMAKE_BINARY_DIR}/src/scaler/utility/queues
        RUNTIME_OUTPUT_DIRECTORY_DEBUG ${CMAKE_BINARY_DIR}/src/scaler/utility/queues
        RUNTIME_OUTPUT_DIRECTORY_RELWITHDEBINFO ${CMAKE_BINARY_DIR}/src/scaler/utility/queues
        RUNTIME_OUTPUT_DIRECTORY_MINSIZEREL ${CMAKE_BINARY_DIR}/src/scaler/utility/queues
    )

    set_target_properties(py_many_to_many_dict PROPERTIES
        PREFIX ""
        OUTPUT_NAME "many_to_many_dict"
        SUFFIX ".pyd"
        LINKER_LANGUAGE CXX
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/src/scaler/utility
        RUNTIME_OUTPUT_DIRECTORY_RELEASE ${CMAKE_BINARY_DIR}/src/scaler/utility
        RUNTIME_OUTPUT_DIRECTORY_DEBUG ${CMAKE_BINARY_DIR}/src/scaler/utility
        RUNTIME_OUTPUT_DIRECTORY_RELWITHDEBINFO ${CMAKE_BINARY_DIR}/src/scaler/utility
        RUNTIME_OUTPUT_DIRECTORY_MINSIZEREL ${CMAKE_BINARY_DIR}/src/scaler/utility
    )
endif()

install(
    TARGETS py_one_to_many_dict
    RUNTIME DESTINATION scaler/utility
    LIBRARY DESTINATION scaler/utility
    ARCHIVE DESTINATION scaler/utility
)

install(
    TARGETS py_stable_priority_queue
    RUNTIME DESTINATION scaler/utility/queues
    LIBRARY DESTINATION scaler/utility/queues
    ARCHIVE DESTINATION scaler/utility/queues
)

install(
    TARGETS py_many_to_many_dict
    RUNTIME DESTINATION scaler/utility
    LIBRARY DESTINATION scaler/utility
    ARCHIVE DESTINATION scaler/utility
)
