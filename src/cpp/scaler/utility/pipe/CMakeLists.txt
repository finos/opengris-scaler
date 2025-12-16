target_sources(scaler_utility PRIVATE
    pipe.h
    pipe_utils.h
    pipe_reader.h
    pipe_reader.cpp
    pipe_writer.h
    pipe_writer.cpp
)

if(UNIX)
    target_sources(scaler_utility PRIVATE
        pipe_utils_unix.cpp
        pipe_reader_unix.cpp
        pipe_writer_unix.cpp
    )
elseif(WIN32)
    target_sources(scaler_utility PRIVATE
        pipe_utils_windows.cpp
        pipe_reader_windows.cpp
        pipe_writer_windows.cpp
    )
endif()
