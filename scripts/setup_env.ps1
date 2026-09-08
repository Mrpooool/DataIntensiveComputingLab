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
    if (Get-Command py -ErrorAction SilentlyContinue) {
        py -3.11 -m venv $Environment
    }
    elseif (Get-Command python -ErrorAction SilentlyContinue) {
        python -m venv $Environment
    }
    else {
        throw "Python 3.11 was not found. Install it and reopen PowerShell."
    }
    $Python = $VenvPython
}

$PythonVersion = & $Python -c "import platform; print(platform.python_version())"
if ($PythonVersion -ne "3.11.9") {
    throw "Expected Python 3.11.9, found $PythonVersion"
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
