#!/bin/bash

# Start a PyFaaS cluster composed of:
#   - One Director
#   - Two Workers

# Path to the Python virtual env
VENV="venv/Scripts/activate.bat"

# Components paths
$DIRECTOR = "src/pyfaas_director/app/pyfaas_director.py"
$WORKER = "src/pyfaas_worker/app/pyfaas_worker.py"


# Starts a Python script in a new terminal window
run_in_new_terminal() {
    local venv=$1
    local script=$2
    gnome-terminal -- bash -c "source '$venv'; python '$script'; exec bash"
}

# Running scripts with a 0.5s delay between each one
run_in_new_terminal "$VENV" "$DIRECTOR"
sleep 0.5

run_in_new_terminal "$VENV" "$WORKER"
sleep 0.5

run_in_new_terminal "$VENV" "$WORKER"
