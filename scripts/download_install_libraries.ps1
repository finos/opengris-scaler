# Constants
$BOOST_VERSION = "1.88.0"
$CAPNP_VERSION = "1.1.0"
$PREFIX = "C:\Program Files"

# Parse optional --prefix argument from $args
foreach ($arg in $args) {
    if ($arg -match "^--prefix=(.+)$") {
        $PREFIX = $matches[1]
    }
}

# Get the number of cores
$NUM_CORES = [Environment]::ProcessorCount

[Environment]::SetEnvironmentVariable("Path",
  [Environment]::GetEnvironmentVariable("Path",
    [EnvironmentVariableTarget]::Machine) + ";$PREFIX",
    [EnvironmentVariableTarget]::Machine)

# Main logic
if ($args.Count -lt 2) {
    Write-Host "Usage: .\download_install_libraries.ps1 [boost|capnp] [compile|install] [--prefix=DIR]"
    exit 1
}

$dependency = $args[0]
$action = $args[1]

# Download, compile, or install Boost
if ($dependency -eq "boost") {
    if ($action -eq "compile") {
        $BOOST_FOLDER_NAME = "boost_" + $BOOST_VERSION -replace '\.', '_'
        $BOOST_PACKAGE_NAME = "$BOOST_FOLDER_NAME.tar.gz"
        $url = "https://archives.boost.org/release/$BOOST_VERSION/source/$BOOST_PACKAGE_NAME"

        # Download and extract Boost
        # Necessary exe because of local dev env
        curl.exe -O $url --retry 100 --retry-max-time 3600
        tar -xzf $BOOST_PACKAGE_NAME
        Rename-Item -Path $BOOST_FOLDER_NAME -NewName "boost"
    }
    elseif ($action -eq "install") {
        Copy-Item -Recurse -Path "boost\boost" -Destination "$PREFIX\include\boost"
        Write-Host "Installed Boost into $PREFIX\include\boost"
    }
    else {
        Write-Host "Argument needs to be either compile or install"
        exit 1
    }
}

# Download, compile, or install Cap'n Proto
elseif ($dependency -eq "capnp") {
    if ($action -eq "compile") {
        $CAPNP_FOLDER_NAME = "capnproto-c++-$CAPNP_VERSION"
        $CAPNP_PACKAGE_NAME = "$CAPNP_FOLDER_NAME.tar.gz"
        $url = "https://capnproto.org/$CAPNP_PACKAGE_NAME"

        # Download and extract Cap'n Proto
        curl.exe -O $url --retry 100 --retry-max-time 3600
        tar -xzf $CAPNP_PACKAGE_NAME
        Rename-Item -Path $CAPNP_FOLDER_NAME -NewName "capnp"

        # Configure and build with Visual Studio using CMake
        Set-Location -Path "capnp"
        cmake -G "Visual Studio 17 2022" -B build
        cmake --build build --config Release
    }
    elseif ($action -eq "install") {
        $CAPNP_FOLDER_NAME = "capnproto-c++-$CAPNP_VERSION"
        Rename-Item -Path $CAPNP_FOLDER_NAME -NewName "capnp"
        Set-Location -Path "capnp"
        cmake --install build --config Release --prefix $PREFIX
        Write-Host "Installed capnp into $PREFIX"
    }
    else {
        Write-Host "Argument needs to be either compile or install"
        exit 1
    }

else {
    Write-Host "Usage: .\download_install_libraries.ps1 [boost|capnp] [--prefix=DIR]"
    exit 1
}

