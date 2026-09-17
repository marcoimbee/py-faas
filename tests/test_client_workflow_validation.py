import pytest

from pyfaas.exceptions import PyFaaSWorkflowValidationError
from pyfaas.util.client_side_workflow_validation import validate_json_workflow_structure


def _base_workflow():
    return {
        'id': 'wf1',
        'entry_function': 'add',
        'functions': {
            'add': {
                'positional_args': [5, 10],
                'default_args': {'c': 26},
                'next': 'multiply',
                'cache_result': True,
            },
            'multiply': {
                'positional_args': ['$add.output', 10],
                'default_args': {},
                'next': '',
                'cache_result': False,
            },
        },
    }


def test_valid_workflow_passes():
    validate_json_workflow_structure(_base_workflow())


def test_empty_workflow_raises():
    with pytest.raises(PyFaaSWorkflowValidationError):
        validate_json_workflow_structure({})


@pytest.mark.parametrize('mutate, match', [
    (lambda wf: wf.pop('id'), 'id'),
    (lambda wf: wf.update(id=123), 'must be of type string'),
    (lambda wf: wf.pop('entry_function'), 'entry_function'),
    (lambda wf: wf.update(entry_function=123), 'must be of type string'),
    (lambda wf: wf.pop('functions'), 'functions'),
    (lambda wf: wf.update(functions=['x']), 'not a dictionary'),
])
def test_top_level_field_errors(mutate, match):
    wf = _base_workflow()
    mutate(wf)
    with pytest.raises(PyFaaSWorkflowValidationError, match=match):
        validate_json_workflow_structure(wf)


def test_entry_function_not_in_functions_raises():
    wf = _base_workflow()
    wf['entry_function'] = 'does_not_exist'
    with pytest.raises(PyFaaSWorkflowValidationError, match='missing in'):
        validate_json_workflow_structure(wf)


@pytest.mark.parametrize('field', ['cache_result', 'positional_args', 'default_args', 'next'])
def test_missing_required_function_field_raises(field):
    wf = _base_workflow()
    del wf['functions']['add'][field]
    with pytest.raises(PyFaaSWorkflowValidationError, match=f"'{field}'"):
        validate_json_workflow_structure(wf)


@pytest.mark.parametrize('field, bad_value', [
    ('cache_result', 'yes'),
    ('positional_args', 'not-a-list'),
    ('default_args', ['not-a-dict']),
    ('next', 123),
])
def test_wrong_type_function_field_raises(field, bad_value):
    wf = _base_workflow()
    wf['functions']['add'][field] = bad_value
    with pytest.raises(PyFaaSWorkflowValidationError, match='must be of type'):
        validate_json_workflow_structure(wf)


def test_next_pointing_to_unknown_function_raises():
    wf = _base_workflow()
    wf['functions']['add']['next'] = 'ghost'
    with pytest.raises(PyFaaSWorkflowValidationError, match='does not point to a valid function'):
        validate_json_workflow_structure(wf)


def test_self_cycle_raises():
    wf = _base_workflow()
    wf['functions']['add']['next'] = 'add'
    with pytest.raises(PyFaaSWorkflowValidationError, match='Self cycle'):
        validate_json_workflow_structure(wf)


def test_unknown_field_raises():
    wf = _base_workflow()
    wf['functions']['add']['unexpected'] = True
    with pytest.raises(PyFaaSWorkflowValidationError, match='Unknown field'):
        validate_json_workflow_structure(wf)


def test_no_terminal_function_raises():
    wf = _base_workflow()
    wf['functions']['multiply']['next'] = 'add'  # now both point somewhere, none end with ''
    with pytest.raises(PyFaaSWorkflowValidationError, match='final function'):
        validate_json_workflow_structure(wf)


def test_unreachable_function_raises():
    wf = _base_workflow()
    wf['functions']['orphan'] = {
        'positional_args': [],
        'default_args': {},
        'next': '',
        'cache_result': False,
    }
    with pytest.raises(PyFaaSWorkflowValidationError, match='unreachable'):
        validate_json_workflow_structure(wf)


def test_bad_positional_arg_reference_raises():
    wf = _base_workflow()
    wf['functions']['multiply']['positional_args'] = ['$ghost.output', 10]
    with pytest.raises(PyFaaSWorkflowValidationError, match='reference error'):
        validate_json_workflow_structure(wf)


def test_empty_function_name_raises():
    wf = _base_workflow()
    wf['functions'][''] = wf['functions'].pop('multiply')
    wf['functions']['add']['next'] = ''
    with pytest.raises(PyFaaSWorkflowValidationError, match='Empty function name'):
        validate_json_workflow_structure(wf)


def test_bad_default_arg_reference_raises():
    wf = _base_workflow()
    wf['functions']['multiply']['default_args'] = {'c': '$ghost.output'}
    with pytest.raises(PyFaaSWorkflowValidationError, match='reference error'):
        validate_json_workflow_structure(wf)
