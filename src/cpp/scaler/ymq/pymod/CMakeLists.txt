scaler_add_python_module(
    TARGET py_ymq
    MODULE_NAME "_ymq"
    INSTALL_DEST scaler/io/ymq
    SOURCES
        address.h
        binder_socket.h
        bytes.h
        connector_socket.h
        exception.h
        io_context.h
        message.h
        ymq.h
        ymq.cpp
    LINK_LIBRARIES
        ymq_objs
)
