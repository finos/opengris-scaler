add_subdirectory(wrapper/uv)

if(LINUX OR APPLE)
  add_subdirectory(object_storage)
else()
  message(WARNING "Not building OSS and its Python Interface, as it's not supported on this system currently!")
endif()

if(LINUX OR WIN32)
  add_subdirectory(ymq)
else()
  message(WARNING "Not building YMQ, as it's not supported on this system currently!")
endif()
