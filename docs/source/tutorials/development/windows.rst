==========================
Windows Development Setup
==========================

.. contents:: Table of Contents
    :depth: 2

Prerequisites
-------------

MSVC v143 C++ Build Tools
~~~~~~~~~~~~~~~~~~~~~~~~~

1. Download the `VS 2022 Build Tools installer <https://aka.ms/vs/17/release/vs_BuildTools.exe>`_.
2. In the **Individual Components** tab, select:

   * ``MSVC v143 - VS 2022 C++ build tools (Latest)``

CMake, Python, and Perl
~~~~~~~~~~~~~~~~~~~~~~~

From an **Administrator Developer PowerShell for VS 2022**, allow script execution and install the remaining tools:

.. code:: powershell

    Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

    winget install Kitware.CMake
    winget install Python.Python.3.12
    winget install StrawberryPerl.StrawberryPerl

Third-Party Libraries
~~~~~~~~~~~~~~~~~~~~~

From the project root, download, compile, and install Cap'n Proto, libuv, and OpenSSL:

.. code:: powershell

    .\scripts\library_tool.ps1 capnp download
    .\scripts\library_tool.ps1 libuv download
    .\scripts\library_tool.ps1 openssl download

    .\scripts\library_tool.ps1 capnp compile
    .\scripts\library_tool.ps1 libuv compile
    .\scripts\library_tool.ps1 openssl compile

    .\scripts\library_tool.ps1 capnp install
    .\scripts\library_tool.ps1 libuv install
    .\scripts\library_tool.ps1 openssl install

Build C++ Components
--------------------

Use the ``build.ps1`` script to compile Scaler's C++ components:

.. code:: powershell

    .\scripts\build.ps1

Install and Run Tests
---------------------

Set up ``uv``, install the package in editable mode, and run the unit tests:

.. code:: powershell

    pip install uv
    python -m pip install -e .
    python -m unittest discover -v tests -t .
