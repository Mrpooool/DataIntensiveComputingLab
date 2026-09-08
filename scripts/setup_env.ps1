$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$Environment = Join-Path $ProjectRoot ".venv"
$CondaPython = Join-Path $Environment "python.exe"
$VenvPython = Join-Path $Environment "Scripts\python.exe"

if (Test-Path $CondaPython) {
    $Python = $CondaPython
}
elseif (Test-Path $VenvPython) {
    $Python = $VenvPython
}
else {
    if (-not (Get-Command conda -ErrorAction SilentlyContinue)) {
        throw "Conda was not found. Install Miniconda or Anaconda, then reopen PowerShell."
    }
    conda create --prefix $Environment -y -c conda-forge `
        python=3.11.9 openjdk=21 pip
    $Python = $CondaPython
}

$LocalJava = Join-Path $Environment "Library\bin\java.exe"
if (-not (Test-Path $LocalJava) -and -not $env:JAVA_HOME) {
    throw "JDK 21 was not found. Set JAVA_HOME or recreate .venv with the documented Conda command."
}

& $Python -m pip install -r (Join-Path $ProjectRoot "requirements.txt")
& (Join-Path $PSScriptRoot "setup_windows_hadoop.ps1")
& $Python -m pip check
& $Python -c "import pyspark, delta; print('Python/Spark/Delta imports passed')"

Write-Host "Environment is ready: $Python"
