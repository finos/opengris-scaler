# OpenGRIS Scaler AI Agents Guide

## Project Overview

OpenGRIS Scaler is an efficient, lightweight, and reliable distributed computation engine.

- Distributed computing across multiple cores and servers
- Python reference client, scheduler and worker implementation
- Portable Cap'n Proto protocol
- Graph scheduling
- Automated load balancing and task recovery
- Support for nested tasks
- Monitoring tools (CLI and GUI)
- Fast network layer written in C++ (YMQ)
- Fast object storage server written in C++

## Key Technologies

- **Python 3.8+**: Primary implementation language
- **C++20**: Performance-critical components
- **Cap'n Proto**: Serialization and messaging protocol
- **asyncio/libuv**: Asynchronous event loops (Python / C++)
- **CMake**: Build system for C++ components
- Python code uses the **unittest** module while C++ code uses **GTest**

## Project Structure

```
scaler/
├── src/
│   ├── scaler/              # Python source code
│   │   ├── client/
│   │   ├── scheduler/
│   │   ├── worker/
│   │   ├── entry_points/    # CLI entry points
│   │   ├── protocol/        # Protocol definitions (Cap'n Proto)
│   │   ├── utility/
│   │   └── worker_adapter/
│   └── cpp/scaler/          # C++ components
│       ├── object_storage/
│       └── ymq/             # Custom messaging queue protocol
├── tests/
│   ├── client/
│   ├── scheduler/
│   ├── cpp/                 # C++ tests
│   └── .../
└── scripts/                 # Build and utility scripts
├── examples/
├── benchmarks/
├── docs/
```

## Coding Standards

Both C++ and Python code share these standards:

- 4 spaces for indentation
- Maximum line length is 120 characters
- Traditional OOP inheritance is highly discouraged, composition is preferred. Prefer interfaces, abstract classes
  and/or mixins
- Explicit naming is preferred. Avoid abbreviations unless widely understood (e.g., `msg`)
- Avoid single letter variables (e.g. `c`), except for `i`, `j` and `n` when used as iteration variables/boundaries
- Avoid magic numbers, prefer constants
- Matching of naming of files, tests, namespaces/modules and directories is highly encouraged

### Python Code

1. **Style Guide**:
   - Follow PEP 8
   - Always include parameter types and return type hints

2. **Imports**:
   - Organize imports in the following order, then sort alphabetically:
      - Standard library imports
      - Third-party imports
      - Local application imports

3. **Naming Conventions**:
   - Classes: `PascalCase`. Capitalize all letters in acronyms (e.g. `HTTPRequest` not `HttpRequest`)
   - Functions/methods: `snake_case`
   - Constants: `UPPER_SNAKE_CASE`
   - Private members: prefix with `_`

### C++ Code

1. **Style Guide**:
   - Follow the project's `.clang-format`:
     - Left pointer alignment (`int* ptr`)
     - Break after function declarations/definitions (opening brace on new line)
     - One parameter per line for function declarations if it exceeds the max. line length
     - Align consecutive assignments

2. **Headers**:
   - Use `#pragma once`
   - Organize includes in the following order, then sort alphabetically:
      - Associated header
      - C/C++ libraries
      - Local includes

      ```c++
      // file is my_class.cpp
      #include "scaler/ymq/my_class.h"

      #include <cstdint>
      #include <memory>

      #include "scaler/error/error.h"
      #include "scaler/ymq/event_loop_thread.h"
      ```

3. **Naming Conventions**:
   - Classes: `PascalCase`. Capitalize all letters in acronyms (e.g. `HTTPRequest` not `HttpRequest`)
   - Functions/methods/variables: `camelCase`
   - Class fields: `_camelCase`. Prefer private fields accessed through getters/setters.
   - Follow this ordering:
      - `public` before `private` section
      - Nested types then fields, then constructors/destructors, then methods, then static methods

4. **Namespaces**:
   - Use the `scaler::` namespace
   - Try to match the namespace and directory structure
   - Avoid `using namespace`, use fully qualified names instead

5. **Modern C++**:
   - Use C++20 features supported by Clang++, MSVSC++ and GCC
   - Use RAII
   - Prefer smart pointers
   - Prefer `std::optional` over null pointers
   - Prefer `std::expected` over exceptions

6. **Cross-platform code**
   - Use STL and libuv utilities instead of native syscalls
   - Avoid platform-specific `#ifdef`, use CMake conditional compilation, e.g:
       - my_class.h                 # common interface
       - my_class.cpp               # common methods
       - my_class_windows.cpp       # Windows specific
       - my_class_unix.cpp          # macOS / Linux specific

## Development Workflow

### Setting Up Development Environment

```bash
# Setup C++ dependencies
./scripts/library_tool.sh capnp download
./scripts/library_tool.sh capnp compile
./scripts/library_tool.sh capnp install

# Install Python package in development mode
pip install -e .

# Building C++ components
./scripts/build.sh
```

### Testing

```bash
python -m unittest discover  # Python
./scripts/test.sh            # C++
```

When writing tests, try to match the directory and module/namespace structure of the code under test.
