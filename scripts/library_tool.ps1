# Constants
$BOOST_VERSION = "1.88.0"
$CAPNP_VERSION = "1.1.0"

$DOWNLOAD_DIR = ".\downloaded"
$PREFIX = "C:\Program Files"

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
    Write-Host "Usage: .\library_tool.ps1 [boost|capnp] [download|compile|install] [--prefix=DIR]"
    exit 1
}

$dependency = $args[0]
$action = $args[1]

# Download, compile, or install Boost
if ($dependency -eq "boost")
{
    $BOOST_FOLDER_NAME = "boost_" + $BOOST_VERSION -replace '\.', '_'

    if ($action -eq "download")
    {
        mkdir "$DOWNLOAD_DIR" -Force
        $url = "https://archives.boost.org/release/$BOOST_VERSION/source/$BOOST_FOLDER_NAME.tar.gz"
        curl.exe --retry 100 --retry-max-time 3600 -L $url -o "$DOWNLOAD_DIR\$BOOST_FOLDER_NAME.tar.gz"
    }
    elseif ($action -eq "compile")
    {
        tar -xzvf "$DOWNLOAD_DIR\$BOOST_FOLDER_NAME.tar.gz" -C .\
    }
    elseif ($action -eq "install")
    {
        Copy-Item -Recurse -Path "boost\boost" -Destination "$PREFIX\include\boost"
        Write-Host "Installed Boost into $PREFIX\include\boost"
    }
    else
    {
        Write-Host "Argument needs to be download or compile or install"
        exit 1
    }
}

# Download, compile, or install Cap'n Proto
elseif ($dependency -eq "capnp")
{
    $CAPNP_FOLDER_NAME = "capnproto-c++-$CAPNP_VERSION"

    if ($action -eq "download")
    {
        mkdir "$DOWNLOAD_DIR" -Force
        $url = "https://capnproto.org/$CAPNP_FOLDER_NAME.tar.gz"
        curl.exe --retry 100 --retry-max-time 3600 -L $url -o "$DOWNLOAD_DIR\$CAPNP_FOLDER_NAME.tar.gz"
    }
    elseif ($action -eq "compile")
    {
        Remove-Item -Path "$CAPNP_FOLDER_NAME" -Recurse -Force
        tar -xzvf "$DOWNLOAD_DIR\$CAPNP_FOLDER_NAME.tar.gz" -C .\

        # Configure and build with Visual Studio using CMake
        Set-Location -Path "$CAPNP_FOLDER_NAME"
        cmake -G "Visual Studio 17 2022" -B build
        cmake --build build --config Release
    }
    elseif ($action -eq "install")
    {
        Set-Location -Path "$CAPNP_FOLDER_NAME"
        cmake --install build --config Release --prefix $PREFIX
        Write-Host "Installed capnp into $PREFIX"
    }
    else
    {
        Write-Host "Argument needs to be download or compile or install"
        exit 1
    }

}
else {
    Write-Host "Usage: .\library_tool.ps1 [download|boost|capnp] [--prefix=DIR]"
    exit 1
}

