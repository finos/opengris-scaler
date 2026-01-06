add_subdirectory(wrapper/uv)

if(LINUX OR APPLE)
	add_subdirectory(object_storage)
else()
	message(WARNING "Not building OSS tests, as it's not supported on this system currently!")
endif()

add_subdirectory(uv_ymq)
add_subdirectory(ymq)
add_subdirectory(logging)
