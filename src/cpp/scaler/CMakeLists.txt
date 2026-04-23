add_subdirectory(wrapper/uv)

if(NOT EMSCRIPTEN)
  # ymq currently requires std::jthread / threading which Emscripten does not
  # support; skip it until ymq gains a single-threaded / websocket backend for wasm.
  add_subdirectory(ymq)
endif()

add_subdirectory(protocol)

if(NOT EMSCRIPTEN)
  # Server-only data-structure extensions; not used by the WASM client build.
  add_subdirectory(utility)
endif()

if((LINUX OR APPLE) AND NOT EMSCRIPTEN)
  add_subdirectory(object_storage)
elseif(NOT EMSCRIPTEN)
  message(WARNING "Not building OSS and its Python Interface, as it's not supported on this system currently!")
endif()
