# PyFaaS chained execution guide

PyFaaS is able to execute linear chains of functions of arbitrary length. <br>
A <b>workflow</b> can be defined, with the specifications of such chain.

## Workflows
An example of workflow is the following JSON structure:
```json
{
    "id": "abc1234",
    "entry_function": "add",
    "functions": {
        "add": {
            "positional_args": [5, 10],
            "default_args": {
                "c": 26
            },
            "next": "multiply",
            "cache_result": true
        },
        "multiply": {
            "positional_args": [13, 4],
            "default_args": {
                "c": "$add.output"
            },
            "next": "divide",
            "cache_result": false
        },
        "divide": {
            "positional_args": ["$multiply.output", 20],
            "default_args": {},
            "next": "",
            "cache_result": true
        }
    }
}
```
The above workflow specifies a chain of three functions:
```bash
add -> multiply -> divide
```

Which are referred to the following functions:
```python
def add(a: int, b: int, c: int = 12) -> int:
    return a + b + c

def multiply(a: int, b: int, c: int = 45) -> int:
    return a * b * c

def divide(a: int, b: int) -> float:
    return a / b
```
Upon workflow execution, `add` will be started, with positional argumens `[5, 10]` and no default arguments. The result of this function will then be `5 + 10 + 26 = 41`. The result of `add` is passed to the next function, `multiply`, via the reference `$add.output`. Such reference tells PyFaaS that the default argument `c` of `multiply` will take as value the output of `add`. <br>
The `multiply` function is then executed with its arguments: `[13, 4]` (positional), and `{c: 41}` from `add` (default). The result will be `13 * 4 * 41 = 2132`. <br>
Following the same rational as before, the function `divide` will receive `multiply`'s output as its first positional argument. It will compute its result `2132 / 20 = 106.6`. <br>
Being `divide` the last function of the chain, its result will be delivered back to the client.

In each workflow, the following fields are required:
- `id`: an identifier of the workflow.
- `entry_function`: the name of the first function in the chain.
- `functions`: an object containing multiple function objects. In the example: `add`, `multiply`, `divide`.

Each function object requires the following fields:
- `positional_args`: a `list` containing the provided positional arguments for the function. The types must be compliant with the type annotation specified at the moment of function registration (referring to the example, `add` must receive as positional arguments a list containing two `int`s, as the type annotations say in its signature).
- `default_args`: a `dict` containing name and value of each default argument that is being specified for the function. If a function signature defines a default argument, and no default argument value is provided for such argument in the workflow, the default value will be used ini the execution.
- `next`: the next function in the chain. This field must be left empty if the function is the final one in the chain.
- `cache_result`: a boolean telling PyFaaS' worker whether to cache the result of the function execution or not. 

### Workflow template creation tool
A workflow template can be created with the specified number of functions in the chain by using the provided `tools/create_workflow.py` tool:
```bash
cd pyfaas/tools
python create_workflow.py -n my_workflow.json -p . -f 5
```
Creates a `workflow_template.json` file with a workflow skeleton, where:
- `-n`: name of the workflow (default to `workflow_template.json`)
- `-p`: destination path (default to `pyfaas/tools`)
- `-f`: number of functions in the chain


## API
To enable chained execution of functions, two functions can be used:
- `pyfaas_load_workflow`
- `pyfaas_chain_exec`

### `pyfaas_load_workflow`:
Can be used to load a JSON workflow from a file:
```python
try:
    workflow_file = 'workflow.json'
    json_workflow = pyfaas_load_workflow(workflow_file)
except Exception as e:
    print(e)
```

### `pyfaas_chain_exec`:
Can be used to execute a chain of function, given a workflow:
```python
try:
    chain_result = pyfaas_chain_exec(json_workflow)
    print(chain_result)
except Exception as e:
    print(e)
```
The `json_workflow` can also be provided in string form, the only requirement is that it needs to be valid JSON.

Of course, to execute a workflow, all the functions in it must be registered using `pyfaas_register` beforehand.