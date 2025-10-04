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
log_directory = "worker/logs"
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


# Examples

## Function registration/unregistration
```python
from pyfaas import pyfaas_register, pyfaas_unregister

def simple_function_1(a: int, b: int, c: int = 12) -> int:
    return a * b + c

pyfaas_register(simple_function_1)      # Registers 'simple_function_1' at the worker's
```
If a previously registered function changes code or arguments, but not its name, can be registered at the worker's by using again `pyfaas_register`, and by specifying `override=True` as an argument. This will override the previously registered function with the same name:
```python
def simple_function_1(d: int) -> float:
    return d + 0.1

pyfaas_register(simple_function_1, override=True)
```
To unregister a function:
```python
# Unregisters 'simple_function_1' from the worker. 
# To invoke it, it must be registered again
pyfaas_unregister('simple_function_1')
```

## Remote function execution
A function can be remotely executed on the worker only if it has been previously registered via a call to `pyfaas_register`.
To remotely execute a function and obtain back its result:
```python
from pyfaas import pyfaas_register, pyfaas_unregister

def simple_function_1(a: int, b: int, c: int = 12) -> int:
    return a * b + c

pyfaas_register(simple_function_1, override=True)

args = [5, 6]
kwargs = {'c': 56}
try:
    res = pyfaas_exec('simple_function_1', args, kwargs)
    print(res)
except Exception as e:
    print(e)
```
### Results caching
If caching is enabled, functions' execution results can be cached at the worker: if a call to a previously registered function happens again with the same args-kwargs combination and such combination is stored in cache, the worker will not execute again such function, but will instead return directly the cache-extracted result to the client. <br>
Saving a function-args-kwargs execution result in the worker's cache cane be enabled by passing `save_in_cache=True` to `pyfaas_exec`:
```python
args = [5, 6]
kwargs = {'c': 56}
try:
    # First time invoking this function with these args and kwargs, 
    # execute and store the result in the worker's cache
    res = pyfaas_exec('simple_function_1', args, kwargs, save_in_cache=True)
    print(res)
except Exception as e:
    print(e)

args = [5, 6]
kwargs = {'c': 56}
try:
    # The result is already in cache, 
    # the worker doesn't execute the function but returns the cache-stored result
    res = pyfaas_exec('simple_function_1', args, kwargs)
    print(res)
except Exception as e:
    print(e)

args = [10, 11]
kwargs = {'c': 90}
try:
    # Another combination of args-kwargs for simple_function_1. 
    # This time the function will be executed and the result saved in cache by the worker
    res = pyfaas_exec('simple_function_1', args, kwargs)
    print(res)
except Exception as e:
    print(e)
```
Caching policy and maximum capcity can be configured via the worker's TOML configuration file.


## Function list
Retrieves information about the currently recorded function at the worker's:
```python
from pyfaas import pyfaas_list

func_list = pyfaas_list()
if func_list != -1:
    print(func_list)
else:
    print('Error retrieving functions')
```

## Statistics retrieval
Retrieves the recorded statistics about functions' execution:
```python
from pyfaas import pyfaas_get_stats

# Retrieving stats for a specifi function
stats = pyfaas_get_stats('simple_function')
if stats != -1:
    print(stats)
else:
    print('Error retrieving functions execution stats')

# No functioin was specified, retrieve all stats
stats = pyfaas_get_stats()
if stats != -1:
    print(stats)
else:
    print('Error retrieving functions execution stats')
```


## Explicit PyFaaS configuration
PyFaaS can be configured client-side by invoking `pyfaas_config`:
```python
from pyfaas import pyfaas_config

config_file_path = <toml-config-file>
pyfaas_config(file_path=config_file_path)
```
If a configuration file is not specified, PyFaaS will resort to a default configuration file that must be found in the project's base directory.

## Kill worker
```python
from pyfaas import pyfaas_kill_worker

pyfaas_kill_worker()
```

## Worker information retrieval
```python
from pyfaas import pyfaas_get_worker_info

info = pyfaas_get_worker_info()
if info != -1:
    print(info)
else:
    print('Error retrieving worker info')
```
If the call is successful, `info` is a `dict` containing information about the worker.

Worker identity info:
- `[identity][ip_address]`: the IP address the worker is reachable at
- `[identity][port]`: the port on which the worker is listening
- `[identity][start_time]`: starting timestamp of the worker
- `[identity][uptime]`: uptime of the worker

Worker system info:
- `[system][python_version]`: Python version of the worker process
- `[system][OS]`: OS of the system hosting the worker
- `[system][CPU]`: CPU specs of the system hosting the worker
- `[system][cores]`: #CPUs of the system hosting the worker

Worker configuration info:
- `[config][enabled_statistics]`: if statistics gathering has been enabled or not
- `[config][log_level]`: logging level of the worker

Functions info:
- `[functions]`: a `dict` containing info about the currently registered functions and their metrics (if enabled)

Networking info:
- `[network][request_count]`: #requests that the worker has received up to that point
- `[network][last_client_connection_timestamp]`: timestamp of the last client connection handled by the worker

## Cache dump inspection
The client is able to inspect the current state of the worker cache by invoking `pyfaas_get_cache_dump`, by which a JSON object is returned:
```python
from pyfaas import pyfaas_get_cache_dump

dump = pyfaas_get_cache_dump()
if dump != -1:
    print(dump)
else:
    print('Error retrieving worker cache dump')
```

## Checking if the worker is alive
This is for testing purposes. Sends a `PING` message to the worker, expecting a `PONG` response:
```python
from pyfaas import pyfaas_ping

pyfaas_ping()
```
