=========================
Dev Container Setup
=========================

.. contents:: Table of Contents
    :depth: 2

Overview
--------

The easiest way to set up a development environment for OpenGRIS Scaler is using the provided dev container.

Prerequisites
-------------

To use the dev container, you'll need:

* `Visual Studio Code <https://code.visualstudio.com/>`_ (or any IDE that supports `dev containers <https://containers.dev/>`_)
* The `Dev Containers extension <https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers>`_ for VS Code
* `Docker <https://www.docker.com/>`_

Getting Started
---------------

1. **Open the Repository**

   Open the OpenGRIS Scaler repository in VS Code.

2. **Reopen in Container**

   When you open the repository, VS Code will detect the dev container configuration and prompt you to reopen in the 
   container. Click **"Reopen in Container"**.
   
   Alternatively, you can manually trigger this by:
   
   * Opening the Command Palette (``Ctrl+Shift+P`` or ``Cmd+Shift+P`` on Mac)
   * Running the command: ``Dev Containers: Reopen in Container``

3. **Build and Install Dependencies**

   Once the container starts, open a terminal in VS Code and run:

   .. code:: bash

       uv sync

   This command will build the project and install all dependencies in a virtual environment located at ``.venv/``.

4. **Activate the Virtual Environment**
   After the project is built and dependencies installed, activate the Python virtual environment with:

   .. code:: bash

       source .venv/bin/activate

   You can now run and debug Scaler from within the dev container.
