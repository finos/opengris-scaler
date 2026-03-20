# Find Python3 development components
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

# Create a C Python extension module
#
# scaler_add_python_module(
#     TARGET <target_name>
#     MODULE_NAME <module_name>
#     INSTALL_DEST <install_path>
#     SOURCES <source1> [<source2> ...]
#     [LINK_LIBRARIES <lib1> [<lib2> ...]]
# )
function(scaler_add_python_module)
    cmake_parse_arguments(
        PYMOD                                           # prefix
        ""                                              # options
        "TARGET;MODULE_NAME;INSTALL_DEST"               # one_value_keywords
        "SOURCES;LINK_LIBRARIES"                        # multi_value_keywords
        ${ARGN}
    )

    if(NOT PYMOD_TARGET OR NOT PYMOD_MODULE_NAME OR NOT PYMOD_INSTALL_DEST OR NOT PYMOD_SOURCES)
        message(FATAL_ERROR "scaler_add_python_module: TARGET, MODULE_NAME, INSTALL_DEST, and SOURCES are required")
    endif()

    # Set output directory so that the library will be installed into the Python source tree.
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/src/${PYMOD_INSTALL_DEST} PARENT_SCOPE)

    add_library(${PYMOD_TARGET} MODULE ${PYMOD_SOURCES})

    # Set basic properties
    set_target_properties(${PYMOD_TARGET} PROPERTIES
        PREFIX ""
        OUTPUT_NAME "${PYMOD_MODULE_NAME}"
        LINKER_LANGUAGE CXX
    )

    if(WIN32)
        # Windows: use .pyd extension and set library output directories.
        set_target_properties(${PYMOD_TARGET} PROPERTIES
            SUFFIX ".pyd"
            LIBRARY_OUTPUT_DIRECTORY                ${CMAKE_BINARY_DIR}/src/${PYMOD_INSTALL_DEST}
            LIBRARY_OUTPUT_DIRECTORY_RELEASE        ${CMAKE_BINARY_DIR}/src/${PYMOD_INSTALL_DEST}
            LIBRARY_OUTPUT_DIRECTORY_DEBUG          ${CMAKE_BINARY_DIR}/src/${PYMOD_INSTALL_DEST}
            LIBRARY_OUTPUT_DIRECTORY_RELWITHDEBINFO ${CMAKE_BINARY_DIR}/src/${PYMOD_INSTALL_DEST}
            LIBRARY_OUTPUT_DIRECTORY_MINSIZEREL     ${CMAKE_BINARY_DIR}/src/${PYMOD_INSTALL_DEST}
        )
    endif()

    target_include_directories(${PYMOD_TARGET} PRIVATE ${PROJECT_SOURCE_DIR}/src/cpp)

    target_link_libraries(${PYMOD_TARGET} PRIVATE Python3::Module)

    if(PYMOD_LINK_LIBRARIES)
        target_link_libraries(${PYMOD_TARGET} PRIVATE ${PYMOD_LINK_LIBRARIES})
    endif()

    install(
        TARGETS ${PYMOD_TARGET}
        RUNTIME DESTINATION ${PYMOD_INSTALL_DEST}
        LIBRARY DESTINATION ${PYMOD_INSTALL_DEST}
        ARCHIVE DESTINATION ${PYMOD_INSTALL_DEST}
    )
endfunction()
