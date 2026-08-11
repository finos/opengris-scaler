# Find Python3 development components for the active build interpreter.
# Rely on CMake/scikit-build selection instead of the system python3-config,
# which can point at a different ABI than the environment running the build.
set(Python3_FIND_VIRTUALENV FIRST)

find_package(Python3 REQUIRED COMPONENTS Interpreter Development.Module)
message(STATUS "Python version: ${Python3_VERSION}")
message(STATUS "Python include dirs: ${Python3_INCLUDE_DIRS}")
message(STATUS "Python ABI: ${Python3_SOABI}")

# Mirrors the config-less IMPORTED_IMPLIB / IMPORTED_LOCATION of a Python3::* imported target into
# Debug and Release per-config slots. Python.org's Windows CMake package only sets the config-less
# values; multi-config generators (Visual Studio) need per-config properties to find the import
# library and runtime DLL for every generated configuration. No-op outside Windows or for targets
# that have not been imported.
function(scaler_patch_python_imported_target target)
    if(NOT WIN32)
        return()
    endif()
    if(NOT TARGET ${target})
        return()
    endif()

    get_target_property(_py_implib ${target} IMPORTED_IMPLIB)
    if(_py_implib)
        set_property(TARGET ${target} PROPERTY IMPORTED_IMPLIB_DEBUG "${_py_implib}")
        set_property(TARGET ${target} PROPERTY IMPORTED_IMPLIB_RELEASE "${_py_implib}")
    endif()

    get_target_property(_py_location ${target} IMPORTED_LOCATION)
    if(_py_location)
        set_property(TARGET ${target} PROPERTY IMPORTED_LOCATION_DEBUG "${_py_location}")
        set_property(TARGET ${target} PROPERTY IMPORTED_LOCATION_RELEASE "${_py_location}")
    endif()

    set_property(TARGET ${target} PROPERTY IMPORTED_CONFIGURATIONS "Debug;Release")
endfunction()

scaler_patch_python_imported_target(Python3::Module)

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

        # Hide all symbols except for the PyInit_<module> entry-point.
        # That avoids symbol collisions when loading multiple modules from the same Python process.
        CXX_VISIBILITY_PRESET hidden
        VISIBILITY_INLINES_HIDDEN ON
    )

    if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID MATCHES "Clang"))
        # Hide symbols from statically linked 3rd-party library (e.g. OpenSSL), avoiding conflicts when loading multiple
        # modules from the same Python process.
        target_link_options(${PYMOD_TARGET} PRIVATE "LINKER:--exclude-libs,ALL")
    endif()

    if(WIN32)
        # Windows: use .pyd extension and set library output directories.
        set_target_properties(${PYMOD_TARGET} PROPERTIES
            SUFFIX ".pyd"
            LIBRARY_OUTPUT_DIRECTORY                ${CMAKE_BINARY_DIR}/src/${PYMOD_INSTALL_DEST}
            LIBRARY_OUTPUT_DIRECTORY_RELEASE        ${CMAKE_BINARY_DIR}/src/${PYMOD_INSTALL_DEST}
            LIBRARY_OUTPUT_DIRECTORY_DEBUG          ${CMAKE_BINARY_DIR}/src/${PYMOD_INSTALL_DEST}
            LIBRARY_OUTPUT_DIRECTORY_RELWITHDEBINFO ${CMAKE_BINARY_DIR}/src/${PYMOD_INSTALL_DEST}
            LIBRARY_OUTPUT_DIRECTORY_MINSIZEREL     ${CMAKE_BINARY_DIR}/src/${PYMOD_INSTALL_DEST}
            MSVC_RUNTIME_LIBRARY                    "MultiThreadedDLL"
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
