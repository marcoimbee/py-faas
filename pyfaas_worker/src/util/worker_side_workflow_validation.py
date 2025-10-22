import inspect

from exceptions import WorkerWorkflowValidationError

# TODO: MISSING CHECKS:
# I am comparing exact types, but what about subclasses, typing.Union, Optional, Any, etc. ?
# None or empty value for a required arg


# Tells, for each type, the types it can be promoted to
# e.g.: if user provides an 'int' as a function arg, and the function's annotation
# is 'float' for that parameter, this should not generate an error, since 'int' can be
# implicitly casted to 'float'
_type_coercion_table = {
    # Numeric types
    'bool': ['int', 'float', 'complex'],
    'int': ['bool', 'float', 'complex'],
    'float': ['bool', 'int', 'complex'],
    'complex': ['bool', 'int', 'float'],
    
    # Text types
    'bytes': ['bytearray'],
    'bytearray': ['bytes'],
}

def _debug_print_args(registered_positional_args, registered_default_args, provided_positional_args, provided_default_args):
    print('Registered positional args: ', registered_positional_args)
    print('Registered default args: ', registered_default_args)
    print('Provided positional args: ', provided_positional_args)
    print('Provided default args: ', provided_default_args)

def _get_registered_positional_args(func_signature):
    return [        # Getting positional arguments name and typing
        [name, param.annotation]                    # [name, type]
        for name, param in func_signature.parameters.items()
        if param.default is inspect._empty      # Filtering out default args
    ]

def _get_registered_default_args(func_signature):
    return [        # Getting default arguments name and typing
        [name, param.annotation, param.default]     # [name, type, default value]
        for name, param in func_signature.parameters.items()
        if param.default is not inspect._empty  # Filtering out positional args
    ]

def validate_function_args(func_code, provided_positional_args, provided_default_args):
    # Getting pre-registered function data
    func_name = func_code.__name__
    registered_func_signature = inspect.signature(func_code)
    registered_positional_args = _get_registered_positional_args(registered_func_signature)
    registered_default_args = _get_registered_default_args(registered_func_signature)

    _debug_print_args(registered_positional_args, registered_default_args, provided_positional_args, provided_default_args)

    # Checking number of provided args against number of registered args
    _check_args_length(provided_positional_args, provided_default_args, registered_positional_args, registered_default_args, func_name)

    # Checking possible arg duplicates
    _check_args_duplicates(provided_positional_args, provided_default_args, registered_positional_args, registered_default_args, func_name)

    # Checking positional args
    _validate_positional_args(provided_positional_args, registered_positional_args, func_name)
    
    # Checking default args
    _validate_default_args(provided_default_args, registered_default_args, func_name)

def _check_args_length(provided_positional_args, provided_default_args, registered_positional_args, registered_default_args, func_name):
    tot_accepted_args = len(registered_positional_args) + len(registered_default_args)
    tot_provided_args = len(provided_positional_args) + len(provided_default_args)
    if tot_provided_args > tot_accepted_args:
        err_msg = f"Function '{func_name}' accepts at most {tot_accepted_args} parameters, while {tot_provided_args} were provided"
        raise WorkerWorkflowValidationError(err_msg)

# Let 'def add(a: int, b: int = 10)' be the function signature
# The workflow provides "positional_args": [5, 20] and "default_args": {"b": 30}
# The param 'b' is already satisfied by the second positional arg (20)
# Providing 'b' in default args again is a duplicate and should raise an error
def _check_args_duplicates(provided_positional_args, provided_default_args, registered_positional_args, registered_default_args, func_name):
    registered_param_names_in_order = [     # Combine param names in declaration order (positional first, then default)
        arg[0] for arg in registered_positional_args + registered_default_args
    ]

    # Determine which registered parameters are filled by positional args
    positional_filled_param_names = registered_param_names_in_order[:len(provided_positional_args)]

    # Finally detect duplicates
    duplicates = set(positional_filled_param_names) & set(provided_default_args.keys())
    if duplicates:
        duplicates_list = ', '.join(sorted(duplicates))
        err_msg = f"Function '{func_name}' has duplicate arguments provided both positionally and by name: {duplicates_list}"
        raise WorkerWorkflowValidationError(err_msg)

def validate_return_type_references(func_code, next_func_code, next_func_provided_positional_args, next_func_provided_default_args):
    # - Is return type of function A compliant with arg of function B, of function B has receives result from function A as input?
    func_name = func_code.__name__
    next_func_name = next_func_code.__name__

    func_signature = inspect.signature(func_code)
    func_return_type = func_signature.return_annotation
    next_func_signature = inspect.signature(next_func_code)
    next_func_registered_positional_args = _get_registered_positional_args(next_func_signature)
    next_func_registered_default_args = _get_registered_default_args(next_func_signature)

    # Checking references in positional args
    for i in range(len(next_func_provided_positional_args)):
        if _is_referenced_arg(next_func_provided_positional_args[i]):      # Check only referenced args
            # Get type of the parameter in position i
            next_func_registered_positional_arg_type = next_func_registered_positional_args[i][1]
            # Checking type promotion
            if next_func_registered_positional_arg_type in _type_coercion_table:
                allowed_types = _type_coercion_table.get(next_func_registered_positional_arg_type)
                if next_func_provided_positional_args[i] in allowed_types:
                    continue
            # Check if type is equal to func_return_type
            if next_func_registered_positional_arg_type != func_return_type:
                err_msg = f"Return type of function '{func_name}' ({func_return_type}) is not compliant with positional argument in position {i} of referenced function '{next_func_name}' ({next_func_registered_positional_arg_type})"
                raise WorkerWorkflowValidationError(err_msg)

    # Checking refernces in default args
    for next_func_provided_default_arg_name, next_func_provided_default_arg_value in next_func_provided_default_args.items():
        if _is_referenced_arg(next_func_provided_default_arg_value):        # Check only referenced args
            for next_func_registered_default_arg in next_func_registered_default_args:
                next_func_registered_default_arg_name = next_func_registered_default_arg[0]
                next_func_registered_default_arg_type = next_func_registered_default_arg[1]
                if next_func_registered_default_arg_name == next_func_provided_default_arg_name:    # Found the default arg to check
                    # Checking type promotion
                    if next_func_registered_default_arg_type in _type_coercion_table:
                        allowed_types = _type_coercion_table.get(next_func_registered_default_arg_type)
                        if func_return_type in allowed_types:
                            continue
                    # Finally checking
                    if next_func_registered_default_arg_type != func_return_type:
                        err_msg = f"Return type of function '{func_name}' ({func_return_type}) is not compliant with default argument '{next_func_provided_default_arg_name}' of referenced function '{next_func_name}' ({next_func_registered_default_arg_type})"
                        raise WorkerWorkflowValidationError(err_msg)

def _is_referenced_arg(arg) -> bool:
    if isinstance(arg, str) and arg.startswith('$') and arg.endswith('.output'):
        return True
    return False

def _validate_positional_args(provided_positional_args, registered_positional_args, func_name):
    # - Does function take the specified number of positional args?
    if len(provided_positional_args) != len(registered_positional_args):
        raise WorkerWorkflowValidationError(f"Function '{func_name} accepts {len(registered_positional_args)}' positional arguments, while {len(provided_positional_args)} were provided")
    # - Are types compliant? (Do the specified positional args types match the registered ones?)
    # - Check for possible type promotion
    # - Skip checking positional args that match '$func_name.output' (otherwise always seen as str here)
    for i in range(len(registered_positional_args)):
        registered_arg_name = registered_positional_args[i][0]
        registered_arg_type = registered_positional_args[i][1]
        if registered_arg_type.__name__ in _type_coercion_table:        # Getting exact type name of registered arg from table
            allowed_types = _type_coercion_table.get(registered_arg_type.__name__)      # Getting the associated allowed types
            if type(provided_positional_args[i]).__name__ in allowed_types:     # If provided arg type is in allowed types, skip
                continue
        if _is_referenced_arg(provided_positional_args[i]):
            continue
        if registered_arg_type != type(provided_positional_args[i]):
            raise WorkerWorkflowValidationError(f"Positional argument '{registered_arg_name}' of function '{func_name}' is of type {registered_arg_type}, while {type(provided_positional_args[i])} was provided")

def _validate_default_args(provided_default_args, registered_default_args, func_name):
    # - Does function accept default args?
    if len(registered_default_args) == 0 and len(provided_default_args) != 0:
        raise WorkerWorkflowValidationError(f"Function '{func_name}' does not accept default arguments, {len(provided_default_args)} were provided")
    # - Is the number of provided default args less or equal to the number of registered default args of the function?
    if len(provided_default_args) > len(registered_default_args):
        raise WorkerWorkflowValidationError(f"Function '{func_name}' does accepts at most {len(registered_default_args)} default arguments, while {len(provided_default_args)} were provided")        
    # - Does function have default args as named? (e.g., user passes 'c = 26', but is 'c' in the registered function specification)
    for provided_default_arg_name in provided_default_args.keys():
        if provided_default_arg_name not in [registered_func_default_arg[0] for registered_func_default_arg in registered_default_args]:
            raise WorkerWorkflowValidationError(f"Function '{func_name}' does not accept any default argument named '{provided_default_arg_name}', while one was provided")
    # - Are types compliant? (Do the specified default args types match the registered ones?)
    # - Skip checking default args with value that match '$func_name.output' (otherwise always seen as str here)
    for i in range(len(registered_default_args)):
        registered_arg_name = registered_default_args[i][0]
        registered_arg_type = registered_default_args[i][1]
        for provided_default_arg_name, provided_default_arg_value in provided_default_args.items():
            if registered_arg_type.__name__ in _type_coercion_table:    # Getting exact type name of registered arg from table
                allowed_types = _type_coercion_table.get(registered_arg_type.__name__)      # Getting the associated allowed types
                if type(provided_default_arg_value).__name__ in allowed_types:  # If provided arg type is in allowed types, skip
                    continue
            if _is_referenced_arg(provided_default_arg_value):
                continue
            if registered_arg_name == provided_default_arg_name and registered_arg_type != type(provided_default_arg_value):
                raise WorkerWorkflowValidationError(f"Default argument '{provided_default_arg_name}' of function '{func_name}' is of type {registered_arg_type}, while {type(provided_default_arg_value)} was provided")
