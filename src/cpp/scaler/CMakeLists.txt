add_subdirectory(wrapper/uv)

if(LINUX OR APPLE)
  add_subdirectory(object_storage)
else()
  message(WARNING "Not building OSS and its Python Interface, as it's not supported on this system currently!")
endif()

add_subdirectory(ymq)

add_subdirectory(utility)
