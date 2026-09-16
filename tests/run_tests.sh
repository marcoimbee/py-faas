#!/bin/bash

# Run the PyFaaS test suite using the project's virtual env

venv/Scripts/python.exe -m pytest tests/ -ra "$@"
