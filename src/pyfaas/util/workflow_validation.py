from ..exceptions import PyFaaSWorkflowValidationError


# Validates the workflow structurally. Type checks will then be performed worker-side.
# No need to send this to the worker if it's malformed.
# This just tells of the structure of the workflow is correct
# Expected structure:
# {
#     "entry_function": "add",
#     "functions": {
#         "add": {
#             "args": [5, 10],
#             "kwargs": {
#                 "c": 26
#             },
#             "next": "multiply"
#         },
#         "multiply": {
#             "args": ["$add.output", 10],
#             "kwargs": {},
#             "next": ""
#         }
#     }
# }
def validate_json_workflow_structure(workflow: dict[str, dict[str, object]]) -> None:
    if workflow == {}:
        raise PyFaaSWorkflowValidationError('Empty workflow')
    
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

    allowed_function_fields = ['args', 'kwargs', 'next']    # Accepted fields in each function sub-dict
    next_fields = []
    all_args    = []
    all_kwargs  = []

    for func_name, func_data in functions.items():
        if not func_name:
            raise PyFaaSWorkflowValidationError('Empty function name')

        if not isinstance(func_data, dict):         # Check if function is a valid dict
            raise PyFaaSWorkflowValidationError(f"'{func_name}' must be an object")

        # 'args' field check        
        if 'args' not in func_data:
            raise PyFaaSWorkflowValidationError(f"Missing field 'args' for object '{func_name}'")
        if not isinstance(func_data['args'], list):
            raise PyFaaSWorkflowValidationError(f"Field 'args' for function '{func_name}' must be of type list")
        all_args.append(func_data['args'])

        #'kwargs' field check
        if 'kwargs' not in func_data:
            raise PyFaaSWorkflowValidationError(f"Missing field 'kwargs' for object '{func_name}'")        
        if not isinstance(func_data['kwargs'], dict):
            raise PyFaaSWorkflowValidationError(f"Field 'kwargs' for function '{func_name}' must be of type dict")
        all_kwargs.append(func_data['kwargs'])

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

    # Check unreachability of each function
    for func_name, _ in functions.items():
        if _is_function_unreachable(func_name, next_fields, entry_function):
            raise PyFaaSWorkflowValidationError(f"Function '{func_name}' is unreachable")

    # Check input args references
    arg_references = _extract_arg_references(all_args)          # Filter args and kwargs, get only the reference
    kwarg_references = _extract_kwarg_references(all_kwargs)
    try:
        _check_args_kwargs_references(arg_references, kwarg_references, function_names)
    except Exception as e:
        raise PyFaaSWorkflowValidationError(f"Input arg reference error: {e}. Expected format: '$src_func_name.output'")

# Checks if a function specified in the workflow is unreachable.
# A funciton is unreachable if it's not specified in any of the 
# 'next' fields of the other specified functions.
def _is_function_unreachable(func_name: str, next_list: list[str], entry_func: str) -> bool:
    if func_name == entry_func:
        return False        # Entry function needs to be excluded from this check
    
    return func_name not in next_list

# Checks a function's input arguments and kwargs list structure
# Inputs coming from the previous funciton should be 
# in the form: $src_func_name.output
# E.g.: "args": ["$add.output", 10]
# This function takes as first arg the output of function "add" and the int 10 as the second
# Need to check if the referenced function name is actually a function that has been specified in the workflow definition
def _check_args_kwargs_references(arg_references: list[str], kwarg_references: list[dict], func_names: list[str]) -> None:
    for arg in arg_references:
        arg_func_name = arg.lstrip('$')
        arg_func_name = arg_func_name.rstrip('.output')
        if arg_func_name not in func_names:
            raise Exception(f"Unknown function '{arg_func_name}' in referenced arg '{arg}'")
    for kwarg in kwarg_references:
        kwarg_func_name = kwarg.lstrip('$')
        kwarg_func_name = kwarg_func_name.rstrip('.output')
        if kwarg_func_name not in func_names:
            raise Exception(f"Unknown function '{kwarg_func_name}' in referenced kwarg '{kwarg}'")

def _extract_arg_references(args_list: list[list[object]]) -> list[str]:
    # '$src_func_name.output'
    linearized_args = []
    for args_list in args_list:
        for arg in len(args_list):          # Get all args that are strings, start with '$' and end with '.output'
            if isinstance(arg, str) and arg.startswith('$') and arg.endswith('.output'):
                linearized_args.append(arg)
    return linearized_args

def _extract_kwarg_references(kwargs_list: list[dict]) -> list[str]:
    # '$src_func_name.output'
    linearized_kwargs = []
    for kwargs_list in kwargs_list:
        for kwarg in kwargs_list.values():  # Get all kwargs that are strings, start with '$' and end with '.output'
            if isinstance(kwarg, str) and kwarg.startswith('$') and kwarg.endswith('.output'):
                linearized_kwargs.append(kwarg)
    return linearized_kwargs
