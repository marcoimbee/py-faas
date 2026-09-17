from typing import Any, Union

import pytest

from pyfaas_worker.app.exceptions import WorkerWorkflowValidationError
from pyfaas_worker.app.util.worker_side_workflow_validation import (
    _is_value_of_type,
    validate_function_args,
    validate_return_type_references,
)


def add(a: int, b: int, c: int = 10) -> int:
    return a + b + c


def add_two_defaults(a: int, b: int, c: int = 10, d: int = 20) -> int:
    return a + b + c + d


def greet(name: str) -> str:
    return f'hello {name}'


def produces_int() -> int:
    return 1


def wants_float(x: float):
    pass


def wants_str(x: str):
    pass


def wants_default_float(x: int, y: float = 1.0):
    pass


def test_valid_call_passes():
    validate_function_args(add, [1, 2], {'c': 3})


def test_valid_call_without_default_args_passes():
    validate_function_args(add, [1, 2], {})


def test_too_many_total_args_raises():
    with pytest.raises(WorkerWorkflowValidationError, match='accepts at most'):
        validate_function_args(add, [1, 2, 3], {'c': 4})


def test_duplicate_positional_and_named_arg_raises():
    # 'c' is filled positionally (3rd positional arg) AND passed again by name
    with pytest.raises(WorkerWorkflowValidationError, match='duplicate arguments'):
        validate_function_args(add_two_defaults, [1, 2, 3], {'c': 99})


def test_positional_arg_wrong_type_raises():
    with pytest.raises(WorkerWorkflowValidationError, match='is of type'):
        validate_function_args(add, [1, 'not-an-int'], {})


def test_positional_arg_type_coercion_passes():
    def wants_float_arg(x: float) -> float:
        return x

    validate_function_args(wants_float_arg, [1], {})  # int -> float is allowed


def test_positional_arg_none_raises():
    with pytest.raises(WorkerWorkflowValidationError, match='cannot be None'):
        validate_function_args(add, [1, None], {})


def test_referenced_positional_arg_skips_type_check():
    validate_function_args(add, [1, '$other_func.output'], {})


def test_too_few_positional_args_raises():
    with pytest.raises(WorkerWorkflowValidationError, match='accepts 2'):
        validate_function_args(add, [1], {})


def test_unknown_default_arg_name_raises():
    with pytest.raises(WorkerWorkflowValidationError, match='does not accept any default argument'):
        validate_function_args(add, [1, 2], {'unknown': 5})


def test_function_without_default_args_rejects_any():
    # length check fires before the more specific "doesn't accept defaults" check
    with pytest.raises(WorkerWorkflowValidationError, match='accepts at most'):
        validate_function_args(greet, ['bob'], {'x': 1})


def test_default_arg_wrong_type_raises():
    with pytest.raises(WorkerWorkflowValidationError, match='is of type'):
        validate_function_args(add, [1, 2], {'c': 'not-an-int'})


def test_return_type_reference_compliant_with_coercion_passes():
    validate_return_type_references(produces_int, wants_float, ['$produces_int.output'], {})


def test_return_type_reference_incompatible_raises():
    with pytest.raises(WorkerWorkflowValidationError):
        validate_return_type_references(produces_int, wants_str, ['$produces_int.output'], {})


def test_return_type_reference_checks_default_args_too():
    validate_return_type_references(produces_int, wants_default_float, [1], {'y': '$produces_int.output'})


def test_return_type_reference_unmatched_default_arg_name_is_not_checked():
    # If the referenced default arg's name doesn't match any of next_func's registered
    # default args, the inner loop never finds a match and so never runs the type
    # check -- this call raises nothing here even though 'y' isn't a real parameter of
    # wants_str. (validate_function_args separately rejects unknown default-arg names
    # when wants_str's own args are validated, so the workflow still fails overall --
    # just not via this function.)
    def wants_str(x: str):
        pass

    validate_return_type_references(produces_int, wants_str, [], {'y': '$produces_int.output'})


def test_return_type_reference_default_arg_incompatible_raises():
    def wants_default_str(x: int, y: str = 'a'):
        pass

    with pytest.raises(WorkerWorkflowValidationError):
        validate_return_type_references(produces_int, wants_default_str, [1], {'y': '$produces_int.output'})


@pytest.mark.parametrize('value, expected_type, expected', [
    (1, int, True),
    ('x', int, False),
    (1, Any, True),
    (1, Union[int, str], True),
    ([1, 2], list[int], True),
    ([1, 'x'], list[int], False),
    ({'a': 1}, dict[str, int], True),
    ({'a': 'x'}, dict[str, int], False),
    ((1, 2), tuple[int], True),
    ((1, 'x'), tuple[int], False),
    ({1, 2}, set[int], True),
    ({1, 'x'}, set[int], False),
])
def test_is_value_of_type(value, expected_type, expected):
    assert _is_value_of_type(value, expected_type) is expected


def test_is_value_of_type_empty_annotation_always_matches():
    import inspect
    assert _is_value_of_type('anything', inspect._empty) is True


def test_is_value_of_type_forward_ref_never_matches():
    # isinstance() rejects a ForwardRef with TypeError, so the except-branch fallback
    # compares type(value).__name__ (e.g. 'int') against str(expected_type) (e.g.
    # "ForwardRef('int')") -- those two strings can never be equal, so a ForwardRef
    # annotation always fails validation regardless of the value's actual type.
    from typing import ForwardRef
    assert _is_value_of_type(1, ForwardRef('int')) is False
    assert _is_value_of_type('x', ForwardRef('int')) is False
