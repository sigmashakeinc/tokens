# install.ps1 - install the `tokens` binary on Windows.
#
#   powershell -ExecutionPolicy ByPass -c "irm https://raw.githubusercontent.com/sigmashakeinc/tokens/main/install.ps1 | iex"
#
# The script downloads the Windows release archive from GitHub, checks its
# SHA-256 against checksums.txt from the same release, installs tokens.exe, and
# adds the install directory to your user PATH. If the check fails or the
# checksum entry is missing, nothing is installed.
#
# Settings. Environment variables work with "irm | iex". The switches work when
# you run the saved file, for example: .\install.ps1 -Version 1.2.3
#   $env:VERSION              or -Version X.Y.Z    Release to install (default: latest).
#   $env:INSTALL_DIR          or -InstallDir DIR   Target directory
#                                                  (default: $env:LOCALAPPDATA\Programs\tokens).
#   $env:TOKENS_FROM_FILE     or -FromFile FILE    Install from a local .zip archive.
#   $env:TOKENS_CHECKSUMS     or -Checksums FILE   checksums.txt for -FromFile
#                                                  (default: checksums.txt beside the archive).
#   $env:TOKENS_REPO                               GitHub repository (default: sigmashakeinc/tokens).
#   $env:TOKENS_RELEASE_BASE                       Base URL of the releases. For tests.
#   $env:TOKENS_NO_PATH=1     or -NoPath           Do not change the user PATH.
#
# The script has no param() block, so it also runs through "irm | iex".

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

function Write-Step([string]$Message) { Write-Host $Message }

function Install-Tokens {
    param([string[]]$Arguments)

    # Windows PowerShell 5.1 can default to old TLS versions.
    try {
        [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
    } catch { }

    $repo = if ($env:TOKENS_REPO) { $env:TOKENS_REPO } else { 'sigmashakeinc/tokens' }
    $base = if ($env:TOKENS_RELEASE_BASE) { $env:TOKENS_RELEASE_BASE.TrimEnd('/') } else { "https://github.com/$repo/releases" }
    $version = $env:VERSION
    $installDir = $env:INSTALL_DIR
    $fromFile = $env:TOKENS_FROM_FILE
    $checksumsFile = $env:TOKENS_CHECKSUMS
    $noPath = ($env:TOKENS_NO_PATH -eq '1')

    for ($i = 0; $i -lt $Arguments.Count; $i++) {
        switch -Regex ($Arguments[$i]) {
            '^-Version$' { $i++; $version = $Arguments[$i] }
            '^-InstallDir$' { $i++; $installDir = $Arguments[$i] }
            '^-FromFile$' { $i++; $fromFile = $Arguments[$i] }
            '^-Checksums$' { $i++; $checksumsFile = $Arguments[$i] }
            '^-NoPath$' { $noPath = $true }
            default { throw "install.ps1: unknown option: $($Arguments[$i])" }
        }
    }

    if ($version) { $version = $version.TrimStart('v') }
    if (-not $installDir) { $installDir = Join-Path $env:LOCALAPPDATA 'Programs\tokens' }

    # Only a 64-bit Windows build exists. Windows on ARM runs it through emulation.
    $cpu = if ($env:PROCESSOR_ARCHITEW6432) { $env:PROCESSOR_ARCHITEW6432 } else { $env:PROCESSOR_ARCHITECTURE }
    if ($cpu -notin @('AMD64', 'ARM64')) { throw "install.ps1: unsupported CPU: $cpu" }

    $work = Join-Path ([IO.Path]::GetTempPath()) ("tokens-install-" + [Guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Path $work | Out-Null
    try {
        if ($fromFile) {
            if (-not (Test-Path -LiteralPath $fromFile -PathType Leaf)) { throw "install.ps1: file not found: $fromFile" }
            $archive = (Resolve-Path -LiteralPath $fromFile).Path
            $archiveName = Split-Path -Leaf $archive
            if (-not $checksumsFile) { $checksumsFile = Join-Path (Split-Path -Parent $archive) 'checksums.txt' }
            if (-not (Test-Path -LiteralPath $checksumsFile -PathType Leaf)) {
                throw "install.ps1: no checksums file at $checksumsFile (use -Checksums)"
            }
            $checksumLines = Get-Content -LiteralPath $checksumsFile
        } else {
            $releaseUrl = if ($version) { "$base/download/v$version" } else { "$base/latest/download" }
            Write-Step "Downloading checksums from $releaseUrl"
            $checksumsPath = Join-Path $work 'checksums.txt'
            Invoke-WebRequest -UseBasicParsing -Uri "$releaseUrl/checksums.txt" -OutFile $checksumsPath
            $checksumLines = Get-Content -LiteralPath $checksumsPath
            # The checksum list is the trusted index: it names the archive and its hash.
            $archiveName = $null
            foreach ($line in $checksumLines) {
                if ($line -match '^[0-9a-fA-F]{64}\s+(tokens_\S+_windows_x64\.zip)\s*$') { $archiveName = $Matches[1]; break }
            }
            if (-not $archiveName) { throw 'install.ps1: this release has no archive for windows/x64' }
            $archive = Join-Path $work $archiveName
            Write-Step "Downloading $archiveName"
            Invoke-WebRequest -UseBasicParsing -Uri "$releaseUrl/$archiveName" -OutFile $archive
        }

        # Verify. Fail closed: a missing entry or a mismatch stops the install.
        $expected = $null
        foreach ($line in $checksumLines) {
            if ($line -match '^([0-9a-fA-F]{64})\s+(\S+)\s*$' -and $Matches[2] -eq $archiveName) { $expected = $Matches[1]; break }
        }
        if (-not $expected) { throw "install.ps1: checksums.txt has no valid entry for $archiveName" }
        $actual = (Get-FileHash -Algorithm SHA256 -LiteralPath $archive).Hash
        if ($actual.ToLowerInvariant() -ne $expected.ToLowerInvariant()) {
            throw "install.ps1: SHA-256 mismatch for $archiveName (expected $($expected.ToLowerInvariant()), got $($actual.ToLowerInvariant())). Nothing was installed."
        }
        Write-Step "SHA-256 verified: $($actual.ToLowerInvariant())"

        # Unpack and install.
        $unpack = Join-Path $work 'unpack'
        Expand-Archive -LiteralPath $archive -DestinationPath $unpack -Force
        $exe = Join-Path $unpack 'tokens.exe'
        if (-not (Test-Path -LiteralPath $exe -PathType Leaf)) { throw 'install.ps1: the archive has no tokens.exe' }

        New-Item -ItemType Directory -Force -Path $installDir | Out-Null
        $target = Join-Path $installDir 'tokens.exe'
        $staged = Join-Path $installDir 'tokens.exe.new'
        Copy-Item -LiteralPath $exe -Destination $staged -Force
        try {
            Move-Item -LiteralPath $staged -Destination $target -Force
        } catch {
            Remove-Item -LiteralPath $staged -Force -ErrorAction SilentlyContinue
            throw "install.ps1: cannot replace $target. Stop any running 'tokens serve' and run the installer again."
        }
        Write-Step "Installed: $target"

        if (-not $noPath) {
            $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
            $entries = @()
            if ($userPath) { $entries = $userPath.Split(';') | Where-Object { $_ -ne '' } }
            $normalized = $installDir.TrimEnd('\')
            $present = $false
            foreach ($entry in $entries) { if ($entry.TrimEnd('\') -ieq $normalized) { $present = $true } }
            if (-not $present) {
                $newPath = (@($entries) + $installDir) -join ';'
                [Environment]::SetEnvironmentVariable('Path', $newPath, 'User')
                Write-Step "Added $installDir to your user PATH. Open a new terminal to use it."
            }
            if (($env:Path.Split(';') | Where-Object { $_.TrimEnd('\') -ieq $normalized }).Count -eq 0) {
                $env:Path = "$env:Path;$installDir"
            }
        }

        & $target --version
        if ($LASTEXITCODE -ne 0) { throw 'install.ps1: the installed binary does not run' }
        Write-Step ''
        Write-Step 'Run the service:  tokens serve'
        Write-Step 'Dashboard:        http://127.0.0.1:7913'
    } finally {
        Remove-Item -LiteralPath $work -Recurse -Force -ErrorAction SilentlyContinue
    }
}

Install-Tokens -Arguments $args
