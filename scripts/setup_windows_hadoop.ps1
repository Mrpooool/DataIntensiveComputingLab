$ErrorActionPreference = "Stop"

if ($env:OS -ne "Windows_NT") {
    Write-Host "Windows Hadoop helpers are not required on this operating system."
    exit 0
}

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$Bin = Join-Path $ProjectRoot ".hadoop\bin"
New-Item -ItemType Directory -Force -Path $Bin | Out-Null

# Apache does not publish official winutils binaries. These community Hadoop
# 3.3.6 helpers are pinned by checksum and used only for local Windows I/O.
$Files = @(
    @{
        Name = "winutils.exe"
        Url = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/winutils.exe"
        Sha256 = "496A591EB1E67DF2A620F710D529BA6DDFE1C19149E6647CC4E320BB0EFD8553"
    },
    @{
        Name = "hadoop.dll"
        Url = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/hadoop.dll"
        Sha256 = "D7AB36A68518748CEF142BE2DA5069B4C763C2CD764C1D2E6AC48C7200405BE3"
    }
)

foreach ($File in $Files) {
    $Target = Join-Path $Bin $File.Name
    if (-not (Test-Path $Target)) {
        Invoke-WebRequest -Uri $File.Url -OutFile $Target -UseBasicParsing
    }
    $Actual = (Get-FileHash $Target -Algorithm SHA256).Hash
    if ($Actual -ne $File.Sha256) {
        Remove-Item $Target -Force
        throw "Checksum mismatch for $($File.Name)"
    }
    Write-Host "Verified $($File.Name): $Actual"
}
