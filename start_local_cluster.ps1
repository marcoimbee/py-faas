# Start a PyFaaS cluster composed of:
#   - One Director
#   - Two Workers

# Path to the Python virtual env
$venv = "venv\Scripts\activate.bat"

# Components paths
$directorModule = "pyfaas_director.app.pyfaas_director"
$workerModule = "pyfaas_worker.app.pyfaas_worker"

# Starts a Python script in a new Powershell window
function Start-Python-Script-New-Window($venvPath, $pyScriptPath) {
    $command = "`"$venvPath`"; cd src; python -m $pyScriptPath; pause"
    Start-Process powershell -ArgumentList "-NoExit", "-Command", $command
}

# Running
Start-Python-Script-New-Window $venv $directorModule
Start-Sleep -Milliseconds 2000

Start-Python-Script-New-Window $venv $workerModule
Start-Sleep -Milliseconds 1000

Start-Python-Script-New-Window $venv $workerModule
