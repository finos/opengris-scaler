$ErrorActionPreference = "Stop"

# Constants

$CAPNP_VERSION = "1.1.0"
$UV_VERSION = "1.51.0"
$OPENSSL_VERSION = "4.0.0"

$THIRD_PARTY_DIRECTORY = ".\thirdparties"

$THIRD_PARTY_DOWNLOADED = "$THIRD_PARTY_DIRECTORY\downloaded"
$THIRD_PARTY_COMPILED = "$THIRD_PARTY_DIRECTORY\compiled"

$PREFIX = "C:\Program Files"

function showHelp {
    Write-Host "Usage: .\library_tool.ps1 [capnp|libuv|openssl] [download|compile|install] [--prefix=DIR]"
    exit 1
}

function exitOnError {
    param([scriptblock]$command)
    & $command
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code ${LASTEXITCODE}: $command"
    }
}

function downloadTarGz($url, $folderName) {
    exitOnError { curl.exe --retry 100 --retry-max-time 3600 -L $url -o "$THIRD_PARTY_DOWNLOADED\$folderName.tar.gz" }
    Write-Host "Downloaded $folderName into $THIRD_PARTY_DOWNLOADED\$folderName.tar.gz"
}

function extractTarGz($folderName) {
    Remove-Item -Path "$THIRD_PARTY_COMPILED\$folderName" -Recurse -Force -ErrorAction SilentlyContinue
    exitOnError { tar -xzvf "$THIRD_PARTY_DOWNLOADED\$folderName.tar.gz" -C "$THIRD_PARTY_COMPILED" }
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

if ($action -eq "download")
{
    mkdir "$THIRD_PARTY_DOWNLOADED" -Force
}
elseif ($action -eq "compile")
{
    mkdir "$THIRD_PARTY_COMPILED" -Force
}

# Download, compile, or install Cap'n Proto
if ($dependency -eq "capnp")
{
    $CAPNP_FOLDER_NAME = "capnproto-c++-$CAPNP_VERSION"
    $CAPNP_URL = "https://capnproto.org/$CAPNP_FOLDER_NAME.tar.gz"

    if ($action -eq "download")
    {
        downloadTarGz $CAPNP_URL $CAPNP_FOLDER_NAME
    }
    elseif ($action -eq "compile")
    {
        extractTarGz $CAPNP_FOLDER_NAME

        # Configure and build with Visual Studio using CMake
        $oldDir = Get-Location
        Set-Location -Path "$THIRD_PARTY_COMPILED\$CAPNP_FOLDER_NAME"
        exitOnError {
            cmake -G "Visual Studio 17 2022" -B build `
                -DCMAKE_INSTALL_PREFIX="$PREFIX" `
                -DCMAKE_INSTALL_LIBDIR=lib `
                -DBUILD_TESTING=OFF
        }
        exitOnError { cmake --build build --config Release }
        Write-Host "Compiled capnp into $THIRD_PARTY_COMPILED\$CAPNP_FOLDER_NAME"
        Set-Location $oldDir
    }
    elseif ($action -eq "install")
    {
        $oldDir = Get-Location
        Set-Location -Path "$THIRD_PARTY_COMPILED\$CAPNP_FOLDER_NAME"
        exitOnError { cmake --install build --config Release --prefix $PREFIX }

        $capnpConfigDirectory = Join-Path $PREFIX "lib\cmake\CapnProto"
        $capnpBuildConfigDirectory = Join-Path (Get-Location) "build\cmake"

        if (-not (Test-Path (Join-Path $capnpConfigDirectory "CapnProtoConfig.cmake")))
        {
            mkdir $capnpConfigDirectory -Force | Out-Null
            Copy-Item "$capnpBuildConfigDirectory\CapnProto*.cmake" -Destination $capnpConfigDirectory -Force
            Write-Host "Copied Cap'n Proto CMake package files into $capnpConfigDirectory"
        }

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
    $UV_URL = "https://github.com/libuv/libuv/archive/refs/tags/v$UV_VERSION.tar.gz"

    if ($action -eq "download")
    {
        downloadTarGz $UV_URL $UV_FOLDER_NAME
    }
    elseif ($action -eq "compile")
    {
        extractTarGz $UV_FOLDER_NAME

        # Configure and build with Visual Studio using CMake
        $oldDir = Get-Location
        Set-Location -Path "$THIRD_PARTY_COMPILED\$UV_FOLDER_NAME"
        exitOnError {
            cmake -G "Visual Studio 17 2022" -B build `
                -DCMAKE_INSTALL_PREFIX="$PREFIX" `
                -DBUILD_TESTING=OFF
        }
        exitOnError { cmake --build build --config Release }
        Write-Host "Compiled libuv into $THIRD_PARTY_COMPILED\$UV_FOLDER_NAME"
        Set-Location $oldDir
    }
    elseif ($action -eq "install")
    {
        $oldDir = Get-Location
        Set-Location -Path "$THIRD_PARTY_COMPILED\$UV_FOLDER_NAME"
        exitOnError { cmake --install build --config Release }
        Write-Host "Installed libuv into $PREFIX"
        Set-Location $oldDir
    }
    else
    {
        Write-Host "Argument needs to be download or compile or install"
        showHelp
    }
}

# Download, compile, or install OpenSSL
elseif ($dependency -eq "openssl")
{
    $OPENSSL_FOLDER_NAME = "openssl-$OPENSSL_VERSION"
    $OPENSSL_URL = "https://github.com/openssl/openssl/releases/download/openssl-$OPENSSL_VERSION/$OPENSSL_FOLDER_NAME.tar.gz"

    if ($action -eq "download")
    {
        downloadTarGz $OPENSSL_URL $OPENSSL_FOLDER_NAME
    }
    elseif ($action -eq "compile")
    {
        extractTarGz $OPENSSL_FOLDER_NAME

        $oldDir = Get-Location
        Set-Location -Path "$THIRD_PARTY_COMPILED\$OPENSSL_FOLDER_NAME"
        exitOnError {
            perl Configure VC-WIN64A --prefix="$PREFIX" --libdir=lib no-tests no-shared
        }
        exitOnError { nmake }
        Write-Host "Compiled OpenSSL into $THIRD_PARTY_COMPILED\$OPENSSL_FOLDER_NAME"
        Set-Location $oldDir
    }
    elseif ($action -eq "install")
    {
        $oldDir = Get-Location
        Set-Location -Path "$THIRD_PARTY_COMPILED\$OPENSSL_FOLDER_NAME"
        exitOnError { nmake install_sw }
        Write-Host "Installed OpenSSL into $PREFIX"
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
