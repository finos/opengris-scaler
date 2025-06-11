#include "ymq.h"

PyMODINIT_FUNC PyInit_ymq(void) {
    return PyModuleDef_Init(&ymq_module);
}
