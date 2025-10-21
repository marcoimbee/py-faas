<p align="center">
    <img src="assets/images/logo-ascii-art.png" alt="pyfaas-logo-ascii-art"> 
</p>

# What
PyFaaS is a minimal, lightweight, and easy to use Python library for remote function execution (FaaS, Function as a Service).
Upon deploying a function to a worker, the user is able to then invoke the function to be executed on such worker, receiving back the result.

# Disclaimer
In a FaaS framework, security is of course one of the main concerns to be addressed. <br>
A proper FaaS library that allows clients to remotely requests functions execution would, more or less, follow the following workflow:
1) Client requests function execution to server/worker
2) Server/worker validates such a request
3) Spawns a container (or reuses one of a controlled pool)
4) Executes function in the containerized environment
5) Returns to the client the function result/eventual errors

The most straightforward way to implement such a workflow would be to leverage Docker and Python's Docker container management SDK.

### Current status 
This project is being developed on a Windows 10 laptop.
Running Docker while developing this project significantly impacts system performance, as (just to make an example) Chrome pages take 30-60s to load while the Docker engine is running, and Docker API interactions are very slow in this setup.

Security features <strong>will for sure be implemented in the future:</strong> right now, no suitable developing resources are available.

### Warning
Until then, using this library comes with security risks. Please do not expose it to untrusted clients or networks. I do not, and will not take any responsibility for damages or issues arising from its use.

## TL;DR
This project is experimental and currently lacks proper sandboxing and security isolation. Do <strong>not</strong> expose it to untrusted clients or networks. Use at your own risk. I do not, and will not take any responsibility for damages or issues arising from its use.


# Installation guide

## Requirements
At least `Python v3.8` is required for using this library.

## Installation
```bash
TODO
```
<!-- PyFaaS can be installed with
```bash
pip install pyfaas
```
To be able to use the library, import PyFaaS in you python file:
```py
from pyfaas import <list-of-functions>
``` -->


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

[logging]
log_level = "info"
log_directory = "pyfaas_worker/logs"
log_filename = "worker_log.log"

[behavior]
shutdown_persistence = true

[behavior.caching]
policy = "LRU"
max_size = 10

[misc]
greeting_msg = "Hello brother"
```
- `[network]` section: contains all the necessary networking fields to be able to contact the worker
    - `worker_ip_addr`: the IP address to which the worker will be reachable.
    - `worker_port`: the port to which the worker will be reachable, given the IP address.
    - Given this example file, the worker will be reachable at `192.168.1.12:2000`.
- `[statistics]` section: contains configuration options for the metrics gathering capabilities of the worker
    - `enabled`: if `true`, allows the worker to collect metrics related to functions' execution. If `false`, statistics gathering is disabled.
- `[logging]`: logging configuration options.
    - `[log_level]`: the logging level of the worker on stdout. Logging can be disabled by specifying `""` for this field
    - `[log_directory]`: destination directory of the worker log file. If non-existent, it is created upon worker start.
    - `[log_filename]`: filename of the worker log file. Log lines are dumped in append mode.
- `[behavior]`: worker behavior configuration options (how the worker will behave)
    - `shutdown_persistence`: if `true`, saves the status of the worker when shut down. When restarted, the worker will load the saved status. If `false`, the worker will not save its state, and will be reset at each restart.
    - `[behavior.caching]`: configuration options for function execution caching
        - `policy`: the replacement policy of the cache. For now, only the LRU (Least Recently Used) policy is available.
        - `max_size`: maximum capacity of the cache. If set to 0, caching is disabled: every attempt to add an element to the cache will result in a no-op.
- `[misc]`: miscellaneous configuration options
    - `greeting_msg`: a greeting message that will be printed to stdout when the worker starts (merely for testing purposes).

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


# Guides and examples
Examples on how to use PyFaaS functions can be found [here](docs/examples.md).

# Notes
- The chained execution of functions and their structuring using user-defined workflows are topics that have have been inspired by [this project](https://github.com/edgeless-project/edgeless).
