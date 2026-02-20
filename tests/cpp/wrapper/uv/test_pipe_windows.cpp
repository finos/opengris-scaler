// Windows-specific pipe name prefix
// On Windows, libuv expects the full pipe path: \\.\pipe\name
const char* pipeNamePrefix = "\\\\.\\pipe\\";
