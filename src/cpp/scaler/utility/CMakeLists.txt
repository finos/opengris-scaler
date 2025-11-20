if(LINUX OR APPLE)
    add_library(scaler_utility OBJECT
		pipe/pipe_utils_unix.cpp
		pipe/pipe_reader_unix.cpp
		pipe/pipe_writer_unix.cpp
    )
elseif(WIN32)
    add_library(scaler_utility OBJECT
		pipe/pipe_utils_windows.cpp
		pipe/pipe_reader_windows.cpp
		pipe/pipe_writer_windows.cpp
    )
endif()
