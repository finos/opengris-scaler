if(LINUX OR APPLE)
  add_subdirectory(object_storage)
else()
  message(WARNING "Not building OSS, as it's not supported on this system currently!")
endif()

add_subdirectory(io/ymq)
