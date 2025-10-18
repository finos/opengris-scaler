# We do NOT merge this two files into one for now because it's likely easier to have another test named test_windows_ymq
# Currently it does not support APPLE, nor does it support WIN32
if(LINUX)
    add_test_executable(test_ymq test_ymq.cpp)
endif()

# Later if we decide to promote ymq logging module to a global one, rename this file to
# test_logging and move it to another directory
# Logging test is cross platformed, as it appears
add_test_executable(test_ymq_logging test_ymq_logging.cpp)
