add_subdirectory(object_storage)

if(LINUX)
  add_subdirectory(io/ymq)
else()
  message(WARNING "Not building YMQ, as it's not supported on this system currently!")
endif()
