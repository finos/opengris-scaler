#define PY_SSIZE_T_CLEAN

// on Windows and in debug mode, undefine _DEBUG before including Python.h
// this prevents issues including the debug version of the Python library
#if defined(_WIN32) && defined(_DEBUG)
#undef _DEBUG
#include <Python.h>
#define _DEBUG
#else
#include <Python.h>
#endif

#include <stdio.h>

#include <array>
#include <filesystem>
#include <format>
#include <fstream>
#include <iostream>
#include <sstream>

#include "tests/cpp/ymq/common/testing.h"
#include "tests/cpp/ymq/common/utils.h"

#ifdef _WIN32
#define popen  _popen
#define pclose _pclose
#endif

TestResult returnFailureIfFalse(bool cond, const char* condStr, const char* file, int line, const char* msg)
{
    // Failure: ... (assertion failed) at file:line
    if (!cond) {
        std::cerr << "Failure";
        if (condStr)
            std::cerr << ": " << condStr;
        if (msg)
            std::cerr << " (" << msg << ")";
        else
            std::cerr << " (assertion failed)";
        if (file)
            std::cerr << " at " << file << ":" << line;
        std::cerr << '\n';
        return TestResult::Failure;
    }
    return TestResult::Success;
}

std::wstring discoverPythonHome(std::string command)
{
    // leverage the system's command line to get the current python prefix
    FILE* pipe = popen(std::format("{} -c \"import sys; print(sys.prefix)\"", command).c_str(), "r");
    if (!pipe)
        throw std::runtime_error("failed to start python process to discover prefix");

    std::array<char, 128> buffer {};
    std::string output {};

    size_t n {};
    while ((n = fread(buffer.data(), 1, buffer.size(), pipe)) > 0)
        output.append(buffer.data(), n);

    // remove trailing whitespace
    output.erase(output.find_last_not_of("\r\n") + 1);

    auto status = pclose(pipe);
    if (status < 0)
        throw std::runtime_error("failed to close close process");
    else if (status > 0)
        throw std::runtime_error("process returned non-zero exit code: " + std::to_string(status));

    // assume it's ascii, so we can just cast it as a wstring
    return std::wstring(output.begin(), output.end());
}

void ensurePythonInitialized()
{
    if (Py_IsInitialized())
        return;

    ensurePythonHome();
    Py_Initialize();

    // add the cwd to the path
    {
        PyObject* sysPath = PySys_GetObject("path");
        if (!sysPath)
            throw std::runtime_error("failed to get sys.path");

        PyObject* newPath = PyUnicode_FromString(".");
        if (!newPath)
            throw std::runtime_error("failed to create Python string");

        if (PyList_Append(sysPath, newPath) < 0) {
            Py_DECREF(newPath);
            throw std::runtime_error("failed to append to sys.path");
        }

        Py_DECREF(newPath);
    }

    // release the GIL, the caller will have to acquire it again
    PyEval_SaveThread();
}

void maybeFinalizePython()
{
    PyGILState_STATE gState = PyGILState_Ensure();
    if (!Py_IsInitialized())
        return;

    Py_Finalize();

    // stop compiler from complaining that it's unused
    (void)gState;
}

TestResult runPython(const char* path, std::vector<std::optional<std::string>> argv)
{
    PyGILState_STATE gState = PyGILState_Ensure();

    auto pidStr = std::to_string(getListenerPid());
    argv.insert(argv.begin(), pidStr.c_str());
    argv.insert(argv.begin(), "mitm");

    // set argv
    {
        PyObject* pyArgv = PyList_New(argv.size());
        if (!pyArgv)
            goto exception;

        for (size_t i = 0; i < argv.size(); i++)
            if (argv[i])
                PyList_SET_ITEM(pyArgv, i, PyUnicode_FromString(argv[i].value().c_str()));
            else
                PyList_SET_ITEM(pyArgv, i, Py_None);

        if (PySys_SetObject("argv", pyArgv) < 0)
            goto exception;

        Py_DECREF(pyArgv);
    }

    {
        std::ifstream file(path);
        std::stringstream buffer;
        buffer << file.rdbuf();

        int rc = PyRun_SimpleString(buffer.str().c_str());
        file.close();

        if (rc < 0)
            throw std::runtime_error("failed to run python script");
    }

    PyGILState_Release(gState);
    return TestResult::Success;

exception:
    PyGILState_Release(gState);
    return TestResult::Failure;
}

TestResult runMitm(
    std::string testCase,
    std::string mitmIp,
    uint16_t mitmPort,
    std::string remoteIp,
    uint16_t remotePort,
    std::vector<std::string> extraArgs)
{
    auto cwd = std::filesystem::current_path();
    chdirToProjectRoot();

    // we build the args for the user to make calling the function more convenient
    std::vector<std::optional<std::string>> args {
        testCase, mitmIp, std::to_string(mitmPort), remoteIp, std::to_string(remotePort)};

    for (auto& arg: extraArgs)
        args.push_back(std::move(arg));

    auto result = runPython("tests/cpp/ymq/py_mitm/main.py", args);

    // change back to the original working directory
    std::filesystem::current_path(cwd);
    return result;
}

void testWrapper(std::function<TestResult()> fn, int timeoutSecs, PipeWriter pipeWr, void* hEvent)
{
    TestResult result = TestResult::Failure;
    try {
        result = fn();
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
    } catch (...) {
        std::cerr << "Unknown exception" << std::endl;
    }

    pipeWr.writeAll((char*)&result, sizeof(TestResult));

    signalEvent(hEvent);
}
