# this fetches Google Test, so it must be included first.
if(LINUX OR APPLE)
	add_subdirectory(object_storage)
endif()

add_subdirectory(ymq)
add_subdirectory(logging)
