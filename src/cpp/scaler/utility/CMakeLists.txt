add_library(scaler_utility OBJECT
    error.h
    format.h
    io_result.h
    io_helpers.h
    io_helpers.cpp
    logging.h
    move_only_function.h
    timestamp.h
)

add_subdirectory(pipe)
