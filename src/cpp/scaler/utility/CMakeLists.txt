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

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/src/scaler/utility)

if(LINUX OR APPLE)
    set_target_properties(py_one_to_many_dict PROPERTIES
        PREFIX ""
        OUTPUT_NAME "one_to_many_dict"
        LINKER_LANGUAGE CXX
    )
elseif(WIN32)
    set_target_properties(py_one_to_many_dict PROPERTIES
        PREFIX ""
        OUTPUT_NAME "one_to_many_dict"
        SUFFIX ".pyd"
        LINKER_LANGUAGE CXX
    )
endif()


install(
    TARGETS py_one_to_many_dict
    LIBRARY DESTINATION scaler/utility
)
