# ONGOING
- Test synchronization
- Test everything with proper testing strategies
- Remove all TODOs


# TODO
- Implement other caching strategies: LFU, TTL-based
- Web dashboard
- Docstring for basically everything
- Authentication (see ZMQ CURVE encryption)
- Possibility for full remote code execution on worker
- Dill and bas64 usage:
    - dill can execute arbitrary Python opcodes on load
    - No checksum or signing of the payload
    - Can use cryptographic signatures or hash validation of serialized function
- Replace dill with secure and faster serialization?
- Make director choose a worker based on its proximity?
- Memory limit/sandboxing/time limit:
    - Run each function in a subprocess with timeout and resource limits


### RESILIENCE
- Worker can shutdown mid-operation, what to do?
- Director fault tolerance: store worker data on Redis, restore when up gaain

### API
- Async execution
- Apply func to many inputs in parallel, pyfaas_map?
- Broadcast (run a func on multiple workers, if any, and return multiple results)
- Registering function with same name but different #args and/or type of args (pyfaas_overload() ?)
- Better "kill" function
- Compare functions (tells the differences in #args, type, return type between two functions), can this be useful in any way?

### WORKER
- Additional metrics:
    - Resource usage per execution:
        - CPU time consumed
        - Memory usage peak and avg
        - I/O usage 
- Handling complex types in chained execution:
<!-- # Assume the user wants to put in a workflow a function that for example produces a complex type like an object he defined or like a numpy array that needs to be fed as an input to the following function in the workflow.
# I could allow the user to chain such a function to another intermediate one in the workflow that serializes the result. The following function will take as input the reference to the serialized object.
# Something like this:
# {
#     "id": "abc1234",
#     "entry_function": "add",
#     "functions": {
#         "add": {
#             "positional_args": [5, 10],
#             "default_args": {
#                 "c": 26
#             },
#             "next": "multiply",
#             "serializer": true
#         },
#         "multiply": {
#             "positional_args": ["$add.output", 10],
#             "default_args": {},
#             "next": "divide"
#         },
#         "divide": {
#             "positional_args": ["$multiply.output", 10],
#             "default_args": {},
#             "next": ""
#         }
#     }
# }

# serializer will take the result of add (Assume it is a complex type) and serialize it. multiply will take the input from serialize instead than from add directly -->



### SECURITY - POSTPONED
- Deny network by default.
- Spawn container as non-root with a small memory cap and a strict timeout.
- Use seccomp and make FS read-only.
- Enforce max output size and safe serialization.
- Log & audit every execution.
- Add rate-limiting & quotas.
- Consider a warm pool for perf.
- Plan for secrets and dependency handling (no arbitrary pip).