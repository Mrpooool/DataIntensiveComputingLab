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
        if ($LASTEXITCODE -ne 0) { throw "Virtual environment creation failed." }
    }
    elseif (Get-Command python -ErrorAction SilentlyContinue) {
        python -m venv $Environment
        if ($LASTEXITCODE -ne 0) { throw "Virtual environment creation failed." }
    }
    else {
        throw "Python 3.11 was not found. Install it and reopen PowerShell."
    }
    $Python = $VenvPython
}

$PythonVersion = & $Python -c "import platform; print(platform.python_version())"
if ($LASTEXITCODE -ne 0) { throw "Python version check failed." }
if ($PythonVersion -ne "3.11.9") {
    throw "Expected Python 3.11.9, found $PythonVersion"
}

if (-not $env:JAVA_HOME -and (Test-Path (Join-Path $Environment "Library\bin\java.exe"))) {
    $env:JAVA_HOME = Join-Path $Environment "Library"
}
if (-not $env:JAVA_HOME) { throw "Set JAVA_HOME to your JDK 21 directory." }
$Java = Join-Path $env:JAVA_HOME "bin\java.exe"
if (-not (Test-Path $Java)) { throw "Java was not found at $Java" }
$JavaVersion = & $Java --version
if ($LASTEXITCODE -ne 0) { throw "Java version check failed." }
if (($JavaVersion | Select-Object -First 1) -notmatch '^(openjdk|java) 21(?:[. +]|$)') {
    throw "Expected JDK 21, found $JavaVersion"
}

& $Python -m pip install -r (Join-Path $ProjectRoot "requirements.txt")
if ($LASTEXITCODE -ne 0) { throw "Dependency installation failed." }
& (Join-Path $PSScriptRoot "setup_windows_hadoop.ps1")
& $Python -m pip check
if ($LASTEXITCODE -ne 0) { throw "Dependency check failed." }
& $Python -c "import pyspark, delta; print('Python/Spark/Delta imports passed')"
if ($LASTEXITCODE -ne 0) { throw "Python/Spark/Delta import check failed." }

Write-Host "Environment is ready: $Python"
