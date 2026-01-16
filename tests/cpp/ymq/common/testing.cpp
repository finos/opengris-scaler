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

TestResult return_failure_if_false(bool cond, const char* msg, const char* cond_str, const char* file, int line)
{
    // Failure: ... (assertion failed) at file:line
    if (!cond) {
        std::cerr << "Failure";
        if (cond_str)
            std::cerr << ": " << cond_str;
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

// in the case that there's no msg, delegate
TestResult return_failure_if_false(bool cond, const char* cond_str, const char* file, int line)
{
    return return_failure_if_false(cond, nullptr, cond_str, file, line);
}

std::wstring discover_python_home(std::string command)
{
    // leverage the system's command line to get the current python prefix
    FILE* pipe = popen(std::format("{} -c \"import sys; print(sys.prefix)\"", command).c_str(), "r");
    if (!pipe)
        throw std::runtime_error("failed to start python process to discover prefix");

    std::array<char, 128> buffer {};
    std::string output {};

    size_t n;
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

void ensure_python_initialized()
{
    if (Py_IsInitialized())
        return;

    ensure_python_home();
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

void maybe_finalize_python()
{
    PyGILState_STATE gstate = PyGILState_Ensure();
    if (!Py_IsInitialized())
        return;

    Py_Finalize();

    // stop compiler from complaining that it's unused
    (void)gstate;
}

TestResult run_python(const char* path, std::vector<std::optional<std::string>> argv)
{
    PyGILState_STATE gstate = PyGILState_Ensure();

    auto pid_s = std::to_string(get_listener_pid());
    argv.insert(argv.begin(), pid_s.c_str());
    argv.insert(argv.begin(), "mitm");

    // set argv
    {
        PyObject* py_argv = PyList_New(argv.size());
        if (!py_argv)
            goto exception;

        for (size_t i = 0; i < argv.size(); i++)
            if (argv[i])
                PyList_SET_ITEM(py_argv, i, PyUnicode_FromString(argv[i].value().c_str()));
            else
                PyList_SET_ITEM(py_argv, i, Py_None);

        if (PySys_SetObject("argv", py_argv) < 0)
            goto exception;

        Py_DECREF(py_argv);
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

    PyGILState_Release(gstate);
    return TestResult::Success;

exception:
    PyGILState_Release(gstate);
    return TestResult::Failure;
}

TestResult run_mitm(
    std::string testcase,
    std::string mitm_ip,
    uint16_t mitm_port,
    std::string remote_ip,
    uint16_t remote_port,
    std::vector<std::string> extra_args)
{
    auto cwd = std::filesystem::current_path();
    chdir_to_project_root();

    // we build the args for the user to make calling the function more convenient
    std::vector<std::optional<std::string>> args {
        testcase, mitm_ip, std::to_string(mitm_port), remote_ip, std::to_string(remote_port)};

    for (auto arg: extra_args)
        args.push_back(arg);

    auto result = run_python("tests/cpp/ymq/py_mitm/main.py", args);

    // change back to the original working directory
    std::filesystem::current_path(cwd);
    return result;
}

void test_wrapper(std::function<TestResult()> fn, int timeout_secs, PipeWriter pipe_wr, void* hEvent)
{
    TestResult result = TestResult::Failure;
    try {
        result = fn();
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        result = TestResult::Failure;
    } catch (...) {
        std::cerr << "Unknown exception" << std::endl;
        result = TestResult::Failure;
    }

    pipe_wr.writeAll((char*)&result, sizeof(TestResult));

    signal_event(hEvent);
}
