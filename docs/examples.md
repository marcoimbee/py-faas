# Examples

## Function registration/unregistration
```python
from pyfaas import pyfaas_register, pyfaas_unregister

def simple_function_1(a: int, b: int, c: int = 12) -> int:
    return a * b + c

try:
    # Registers 'simple_function_1' at the worker's
    pyfaas_register(simple_function_1)
except PyFaaSFunctionRegistrationError as e:
    print(e)
```
<u><b>Note that to successfully register a function, all type annotations must be provided, both for parameters and return type.</u></b>

```
To unregister a function:
```python
try:
    # Unregisters 'simple_function_1' from the worker. 
    # To invoke it, it must be registered again
    pyfaas_unregister('simple_function_1')
except PyFaaSFunctionUnregistrationError as e:
    print(e)
```

## Remote function execution
A function can be remotely executed on the worker only if it has been previously registered via a call to `pyfaas_register`.
To remotely execute a function and obtain back its result:
```python
from pyfaas import pyfaas_register, pyfaas_unregister

def simple_function_1(a: int, b: int, c: int = 12) -> int:
    return a * b + c

try:
    pyfaas_register(simple_function_1, override=True)
except PyFaaSFunctionRegistrationError as e:
    print(e)

pos_args = [5, 6]
def_args = {'c': 56}
try:
    res = pyfaas_exec('simple_function_1', pos_args, def_args)
    print(res)
except PyFaaSFunctionExecutionError as e:
    print(e)
```
### Results caching
If caching is enabled, functions' execution results can be cached at the worker: if a call to a previously registered function happens again with the same positional args - default args combination and such combination is stored in cache, the worker will not execute again such function, but will instead return directly the cache-extracted result to the client. <br>
Saving a function - positional args - default args execution result in the worker's cache can be enabled by passing `save_in_cache=True` to `pyfaas_exec`:
```python
pos_args = [5, 6]
def_args = {'c': 56}
try:
    # First time invoking this function with these positional args and default args, 
    # execute and store the result in the worker's cache
    res = pyfaas_exec('simple_function_1', pos_args, def_args, save_in_cache=True)
    print(res)
except PyFaaSFunctionExecutionError as e:
    print(e)

pos_args = [5, 6]
def_args = {'c': 56}
try:
    # The result is already in cache, 
    # the worker doesn't execute the function but returns the cache-stored result
    res = pyfaas_exec('simple_function_1', pos_args, def_args)
    print(res)
except PyFaaSFunctionExecutionError as e:
    print(e)

pos_args = [10, 11]
def_args = {'c': 90}
try:
    # Another combination of positional args - default args for simple_function_1. 
    # This time the function will be executed and the result saved in cache by the worker
    res = pyfaas_exec('simple_function_1', pos_args, def_args)
    print(res)
except PyFaaSFunctionExecutionError as e:
    print(e)
```
Caching policy and maximum capcity can be configured via the worker's TOML configuration file.

## Chained function execution
To understand how to use the provided `pyfaas_chain_exec` function, refer to [this](chain_exec_guide.md) guide.

## Function list
Retrieves information about the currently recorded function at the worker's:
```python
from pyfaas import pyfaas_list

try:
    func_list = pyfaas_list()
    print(func_list)
except PyFaaSFunctionListingError as e:
    print(e)
```

## Statistics retrieval
Retrieves the recorded statistics about functions' execution:
```python
from pyfaas import pyfaas_get_stats

# Retrieving stats for a specifi function
try:
    stats = pyfaas_get_stats('simple_function')
    print(stats)
except PyFaaSStatisticsRetrievalError as e:
    print(e)

# No functioin was specified, retrieve all stats
try:
    stats = pyfaas_get_stats()
    print(stats)
except PyFaaSStatisticsRetrievalError as e:
    print(e)
```

## Explicit PyFaaS configuration
PyFaaS can be configured client-side by invoking `pyfaas_config`:
```python
from pyfaas import pyfaas_config

config_file_path = <toml-config-file>
try:
    pyfaas_config(file_path=config_file_path)
except PyFaaSConfigError as e:
    print(e)
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

try:
    info = pyfaas_get_worker_info()
    print(info)
except PyFaaSWorkerInfoError as e:
    print(e)
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
try:
    print(dump)
except PyFaaSCacheDumpingException as e:
    print(e)
```

## Checking if the worker is alive
This is for testing purposes. Sends a `PING` message to the worker, expecting a `PONG` response:
```python
from pyfaas import pyfaas_ping

pyfaas_ping()
```
