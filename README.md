<p align="center">
    <img src="assets/images/logo-ascii-art.png" alt="pyfaas-logo-ascii-art"> 
</p>

# What
PyFaaS is a minimal, lightweight, and easy to use Python library for remote function execution (FaaS, Function as a Service).
Upon deploying a function to a worker, the user is able to then invoke the function to be executed on such worker, receiving back the result.


# Installation guide

## Requirements
At least `Python v3.8` is required for using this library.

## Installation
PyFaaS can be installed with
```bash
pip install pyfaas
```
To be able to use the library, import PyFaaS in you python file:
```py
from pyfaas import <list-of-functions>
```


# Configuring the client and the worker
Client (using this library) and worker (the entity that remotely executes functions) can be configured by means of TOML configuration files.

## Worker TOML configuration file
The fields of a worker TOML configuration file are the following:
```TOML
[network]
worker_ip_addr = "192.168.1.12"
worker_port = 2000

[statistics]
enabled = true

[misc]
greeting_msg = "Hello brother"
log_level = "info"
```
- `[network]` section: contains all the necessary networking fields to be able to contact the worker
    - `worker_ip_addr`: the IP address to which the worker will be reachable.
    - `worker_port`: the port to which the worker will be reachable, given the IP address.
    - Given this example file, the worker will be reachable at `192.168.1.12:2000`.
- `[statistics]` section: contains configuration options for the metrics gathering capabilities of the worker
    - `enabled`: if `true`, allows the worker to collect metrics related to functions' execution. If `false`, statistics gathering is disabled.
- `[misc]`: miscellaneous configuration options
    - `greeting_msg`: a greeting message that will be printed to stdout when the worker starts (merely for testing purposes).
    - `log_level`: the logging level of the worker on stdout. Logging can be disabled by specifying `""` for this field.

## Client TOML configuration file
The client using the library can setup the communication with a remote worker using a TOML configuration file as well. The fields are the following:
```TOML
[network]
worker_ip_addr = "192.168.1.12"
worker_port = 2000

[misc]
log_level = "info"
```
- `[network]` section: contains all the necessary networking fields to be able to contact the worker
    - `worker_ip_addr`: the IP address to which the worker will be reachable.
    - `worker_port`: the port to which the worker will be reachable, given the IP address.
    - Given this example file, the library will contact a worker reachable at `192.168.1.12:2000`.
- `[misc]`: miscellaneous configuration options
    - `log_level`: the logging level of PyFaaS on stdout. Logging can be disabled by specifying `""` for this field.

# Examples
