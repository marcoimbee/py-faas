from ..exceptions import PyFaaSWorkflowValidationError


# Validates the workflow structurally. Type checks will then be performed worker-side.
# No need to send this to the worker if it's malformed.
# This just tells of the structure of the workflow is correct
# Expected sample structure:
# {
#     "entry_function": "add",
#     "functions": {
#         "add": {
#             "positional_args": [5, 10],
#             "default_args": {
#                 "c": 26
#             },
#             "next": "multiply",
#             "cache_result": true
#         },
#         "multiply": {
#             "positional_args": ["$add.output", 10],
#             "default_args": {},
#             "next": ""
#             "cache_result": false
#         }
#     }
# }
def validate_json_workflow_structure(workflow: dict[str, dict[str, object]]) -> None:
    if workflow == {}:
        raise PyFaaSWorkflowValidationError('Empty workflow')
    
    workflow_id = workflow.get('id')
    if not workflow_id:
        raise PyFaaSWorkflowValidationError('Missing or empty field "id". Workflows must have a string ID')
    if not isinstance(workflow_id, str):
        raise PyFaaSWorkflowValidationError(f'Field "id" must be of type string. Provided type: {type(workflow_id)}')        

    # 'entry_function' field check
    entry_function = workflow.get('entry_function')   # Starting point of the workflow must be provided via 'entry_function' field
    if not entry_function:
        raise PyFaaSWorkflowValidationError('Missing or empty field "entry_function"')
    if not isinstance(entry_function, str):
        raise PyFaaSWorkflowValidationError(f'Field "entry_function" must be of type string. Provided type: {type(entry_function)}')

    # Parsing 'functions' JSON dict
    if not workflow.get('functions'):
        raise PyFaaSWorkflowValidationError('Missing or empty object "functions"')
    functions = workflow.get('functions')
    if not isinstance(functions, dict):
        raise PyFaaSWorkflowValidationError("'functions' is not a dictionary")

    # Extracting function names
    function_names = [func_name for func_name, _ in functions.items()]
    if entry_function not in function_names:
        raise PyFaaSWorkflowValidationError(f"Entry function '{entry_function}' is missing in 'functions'")

    allowed_function_fields = ['positional_args', 'default_args', 'cache_result', 'next']    # Accepted fields in each function sub-dict
    next_fields = []
    all_positional_args    = []
    all_default_args  = []

    for func_name, func_data in functions.items():
        if not func_name:
            raise PyFaaSWorkflowValidationError('Empty function name')

        if not isinstance(func_data, dict):         # Check if function is a valid dict
            raise PyFaaSWorkflowValidationError(f"'{func_name}' must be an object")
        
        # 'cache_result' field check (must be present and a boolean)
        if 'cache_result' not in func_data:
            raise PyFaaSWorkflowValidationError(f"Missing field 'cache_result' for object '{func_name}'")
        if not isinstance(func_data['cache_result'], bool):
            raise PyFaaSWorkflowValidationError(f"Field 'cache_Result' for function '{func_name}' must be of type list")

        # 'positional_args' field check
        if 'positional_args' not in func_data:
            raise PyFaaSWorkflowValidationError(f"Missing field 'positional_args' for object '{func_name}'")
        if not isinstance(func_data['positional_args'], list):
            raise PyFaaSWorkflowValidationError(f"Field 'positional_args' for function '{func_name}' must be of type list")
        all_positional_args.append(func_data['positional_args'])

        #'default_args' field check
        if 'default_args' not in func_data:
            raise PyFaaSWorkflowValidationError(f"Missing field 'default_args' for object '{func_name}'")        
        if not isinstance(func_data['default_args'], dict):
            raise PyFaaSWorkflowValidationError(f"Field 'default_args' for function '{func_name}' must be of type dict")
        all_default_args.append(func_data['default_args'])

        # 'next' field check
        if 'next' not in func_data:
            raise PyFaaSWorkflowValidationError(f"Missing field 'next' for object '{func_name}'")
        if not isinstance(func_data['next'], str):
            raise PyFaaSWorkflowValidationError(f"Field 'next' for function '{func_name}' must be of type string")
        next_fields.append(func_data['next'])
        
        if func_data['next'] not in function_names and func_data['next'] != '':
            raise PyFaaSWorkflowValidationError(f"Field 'next' for function '{func_name}' does not point to a valid function")

        # Check for node's self cycles (A->A)
        if func_data['next'] == func_name:
            raise PyFaaSWorkflowValidationError(f"Self cycle not allowed: field 'next' for function '{func_name}' points to '{func_name}'")

        # Checking for unallowed fields
        for field in func_data.keys():
            if field not in allowed_function_fields:
                raise PyFaaSWorkflowValidationError(f"Unknown field '{field}' in object '{func_name}'")

    # Check presence of a final function (one that has 'next' field equal to '')
    if '' not in next_fields:
        raise PyFaaSWorkflowValidationError(f"Unable to detect final function in chain. No function has field 'next' set to empty")

    # Check unreachability of each function
    for func_name, _ in functions.items():
        if _is_function_unreachable(func_name, next_fields, entry_function):
            raise PyFaaSWorkflowValidationError(f"Function '{func_name}' is unreachable")

    # Check input args references
    positional_arg_references = _extract_positional_arg_references(all_positional_args)    # Filter positional args and default args, get only the reference
    default_args_references = _extract_default_arg_references(all_default_args)
    try:
        _check_pos_args_and_def_args_references(positional_arg_references, default_args_references, function_names)
    except Exception as e:
        raise PyFaaSWorkflowValidationError(f"Input arg reference error: {e}. Expected format: '$src_func_name.output'")

# Checks if a function specified in the workflow is unreachable.
# A funciton is unreachable if it's not specified in any of the 
# 'next' fields of the other specified functions.
def _is_function_unreachable(func_name: str, next_list: list[str], entry_func: str) -> bool:
    if func_name == entry_func:
        return False        # Entry function needs to be excluded from this check
    
    return func_name not in next_list

# Checks a function's input positional arguments and default arguments list structure
# Inputs coming from the previous funciton should be 
# in the form: $src_func_name.output
# E.g.: "args": ["$add.output", 10]
# This function takes as first arg the output of function "add" and the int 10 as the second
# Need to check if the referenced function name is actually a function that has been specified in the workflow definition
def _check_pos_args_and_def_args_references(positional_arg_references: list[str], default_arg_references: list[dict], func_names: list[str]) -> None:
    for pos_arg in positional_arg_references:
        arg_func_name = pos_arg.lstrip('$')
        arg_func_name = arg_func_name.rstrip('.output')
        if arg_func_name not in func_names:
            raise Exception(f"Unknown function '{arg_func_name}' in referenced positional argument '{pos_arg}'")
    for def_arg in default_arg_references:
        def_arg_func_name = def_arg.lstrip('$')
        def_arg_func_name = def_arg_func_name.rstrip('.output')
        if def_arg_func_name not in func_names:
            raise Exception(f"Unknown function '{def_arg_func_name}' in referenced default argument '{def_arg}'")

def _extract_positional_arg_references(positional_args_list: list[list[object]]) -> list[str]:
    # '$src_func_name.output'
    linearized_pos_args = []
    for pos_args_list in positional_args_list:
        for arg in pos_args_list:          # Get all positional args that are strings, start with '$' and end with '.output'
            if isinstance(arg, str) and arg.startswith('$') and arg.endswith('.output'):
                linearized_pos_args.append(arg)
    return linearized_pos_args

def _extract_default_arg_references(default_args_list: list[dict]) -> list[str]:
    # '$src_func_name.output'
    linearized_def_args = []
    for def_args_list in default_args_list:
        for def_arg in def_args_list.values():  # Get all default args that are strings, start with '$' and end with '.output'
            if isinstance(def_arg, str) and def_arg.startswith('$') and def_arg.endswith('.output'):
                linearized_def_args.append(def_arg)
    return linearized_def_args
