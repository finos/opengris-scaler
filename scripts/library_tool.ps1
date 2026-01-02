# Constants
$BOOST_VERSION = "1.88.0"
$CAPNP_VERSION = "1.1.0"
$UV_VERSION = "1.51.0"

$THIRD_PARTY_DIRECTORY = ".\thirdparties"

$THIRD_PARTY_DOWNLOADED = "$THIRD_PARTY_DIRECTORY\downloaded"
$THIRD_PARTY_COMPILED = "$THIRD_PARTY_DIRECTORY\compiled"

$PREFIX = "C:\Program Files"

function showHelp {
    Write-Host "Usage: .\library_tool.ps1 [boost|capnp|libuv] [download|compile|install] [--prefix=DIR]"
    exit 1
}

# Parse optional --prefix argument from $args
foreach ($arg in $args)
{
    if ($arg -match "^--prefix=(.+)$")
    {
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
if ($args.Count -lt 2)
{
    showHelp
}

$dependency = $args[0]
$action = $args[1]

# Download, compile, or install Boost
if ($dependency -eq "boost")
{
    $BOOST_FOLDER_NAME = "boost_" + $BOOST_VERSION -replace '\.', '_'

    if ($action -eq "download")
    {
        mkdir "$THIRD_PARTY_DOWNLOADED" -Force
        $url = "https://archives.boost.org/release/$BOOST_VERSION/source/$BOOST_FOLDER_NAME.tar.gz"
        curl.exe --retry 100 --retry-max-time 3600 -L $url -o "$THIRD_PARTY_DOWNLOADED\$BOOST_FOLDER_NAME.tar.gz"
        Write-Host "Downloaded Boost into $THIRD_PARTY_DOWNLOADED\$BOOST_FOLDER_NAME.tar.gz"
    }
    elseif ($action -eq "compile")
    {
        mkdir "$THIRD_PARTY_COMPILED" -Force
        tar -xzvf "$THIRD_PARTY_DOWNLOADED\$BOOST_FOLDER_NAME.tar.gz" -C "$THIRD_PARTY_COMPILED"
        Write-Host "Compiled Boost into $THIRD_PARTY_COMPILED\$BOOST_FOLDER_NAME"
    }
    elseif ($action -eq "install")
    {
        Copy-Item -Recurse -Path "$THIRD_PARTY_COMPILED\$BOOST_FOLDER_NAME\boost" -Destination "$PREFIX\include\boost"
        Write-Host "Installed Boost into $PREFIX\include\boost"
    }
    else
    {
        Write-Host "Argument needs to be download or compile or install"
        showHelp
    }
}

# Download, compile, or install Cap'n Proto
elseif ($dependency -eq "capnp")
{
    $CAPNP_FOLDER_NAME = "capnproto-c++-$CAPNP_VERSION"

    if ($action -eq "download")
    {
        mkdir "$THIRD_PARTY_DOWNLOADED" -Force
        $url = "https://capnproto.org/$CAPNP_FOLDER_NAME.tar.gz"
        curl.exe --retry 100 --retry-max-time 3600 -L $url -o "$THIRD_PARTY_DOWNLOADED\$CAPNP_FOLDER_NAME.tar.gz"
        Write-Host "Downloaded capnp into $THIRD_PARTY_DOWNLOADED\$CAPNP_FOLDER_NAME.tar.gz"
    }
    elseif ($action -eq "compile")
    {
        Remove-Item -Path "$THIRD_PARTY_DOWNLOADED\$CAPNP_FOLDER_NAME" -Recurse -Force -ErrorAction SilentlyContinue
        mkdir "$THIRD_PARTY_COMPILED" -Force
        tar -xzvf "$THIRD_PARTY_DOWNLOADED\$CAPNP_FOLDER_NAME.tar.gz" -C "$THIRD_PARTY_COMPILED"

        # Configure and build with Visual Studio using CMake
        $oldDir = Get-Location
        Set-Location -Path "$THIRD_PARTY_COMPILED\$CAPNP_FOLDER_NAME"
        cmake -G "Visual Studio 17 2022" -B build
        cmake --build build --config Release
        Write-Host "Compiled capnp into $THIRD_PARTY_COMPILED\$CAPNP_FOLDER_NAME"
        Set-Location $oldDir
    }
    elseif ($action -eq "install")
    {
        $oldDir = Get-Location
        Set-Location -Path "$THIRD_PARTY_COMPILED\$CAPNP_FOLDER_NAME"
        cmake --install build --config Release --prefix $PREFIX
        Write-Host "Installed capnp into $PREFIX"
        Set-Location $oldDir
    }
    else
    {
        Write-Host "Argument needs to be download or compile or install"
        showHelp
    }
}

# Download, compile, or install libuv
elseif ($dependency -eq "libuv")
{
    $UV_FOLDER_NAME = "libuv-$UV_VERSION"

    if ($action -eq "download")
    {
        mkdir "$THIRD_PARTY_DOWNLOADED" -Force
        $url = "https://github.com/libuv/libuv/archive/refs/tags/v$UV_VERSION.tar.gz"
        curl.exe --retry 100 --retry-max-time 3600 -L $url -o "$THIRD_PARTY_DOWNLOADED\$UV_FOLDER_NAME.tar.gz"
        Write-Host "Downloaded libuv into $THIRD_PARTY_DOWNLOADED\$UV_FOLDER_NAME.tar.gz"
    }
    elseif ($action -eq "compile")
    {
        Remove-Item -Path "$THIRD_PARTY_COMPILED\$UV_FOLDER_NAME" -Recurse -Force -ErrorAction SilentlyContinue
        mkdir "$THIRD_PARTY_COMPILED" -Force
        tar -xzvf "$THIRD_PARTY_DOWNLOADED\$UV_FOLDER_NAME.tar.gz" -C "$THIRD_PARTY_COMPILED"

        # Configure and build with Visual Studio using CMake
        $oldDir = Get-Location
        Set-Location -Path "$THIRD_PARTY_COMPILED\$UV_FOLDER_NAME"
        cmake -G "Visual Studio 17 2022" -B build -DCMAKE_INSTALL_PREFIX="$PREFIX" -DBUILD_TESTING=OFF
        cmake --build build --config Release
        Write-Host "Compiled libuv into $THIRD_PARTY_COMPILED\$UV_FOLDER_NAME"
        Set-Location $oldDir
    }
    elseif ($action -eq "install")
    {
        $oldDir = Get-Location
        Set-Location -Path "$THIRD_PARTY_COMPILED\$UV_FOLDER_NAME"
        cmake --install build --config Release
        Write-Host "Installed libuv into $PREFIX"
        Set-Location $oldDir
    }
    else
    {
        Write-Host "Argument needs to be download or compile or install"
        showHelp
    }
}

else {
    showHelp
}

