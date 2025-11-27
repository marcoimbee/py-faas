import pytest
from unittest.mock import MagicMock, patch
import zmq

from pyfaas.pyfaas import pyfaas_chain_exec, _CLIENT_MANAGER
from pyfaas.exceptions import (
    PyFaaSTimeoutError,
    PyFaaSChainedExecutionError,
    PyFaaSWorkflowValidationError
)

# Mock a valid workflow structure for testing
VALID_WORKFLOW = {
    "id": "workflow1",
    "tasks": {
        "start": {"func_id": "func1", "args": [1]},
        "end": {"func_id": "func2", "args": ["$start.result"]},
    }
}

# Mock the validation function dependency
def mock_validate_workflow_success(workflow):
    pass

def mock_validate_workflow_fail(workflow):
    raise PyFaaSWorkflowValidationError("Invalid structure details")

@patch("pyfaas.pyfaas.validate_json_workflow_structure", side_effect=mock_validate_workflow_success)
def test_chain_exec_not_configured(_):
    _CLIENT_MANAGER.configured = False

    with pytest.raises(RuntimeError):
        pyfaas_chain_exec(VALID_WORKFLOW)

@patch("pyfaas.pyfaas.validate_json_workflow_structure", side_effect=mock_validate_workflow_success)
def test_chain_exec_missing_workflow(_):
    _CLIENT_MANAGER.configured = True

    with pytest.raises(PyFaaSChainedExecutionError):
        pyfaas_chain_exec(None)
    
    with pytest.raises(PyFaaSChainedExecutionError):
        pyfaas_chain_exec({})

@patch("pyfaas.pyfaas.validate_json_workflow_structure", side_effect=mock_validate_workflow_fail)
def test_chain_exec_validation_error(_):
    _CLIENT_MANAGER.configured = True

    with patch("pyfaas.pyfaas.logger"):
        with pytest.raises(PyFaaSWorkflowValidationError):
            pyfaas_chain_exec(VALID_WORKFLOW)

@patch("pyfaas.pyfaas.validate_json_workflow_structure", side_effect=mock_validate_workflow_success)
def test_chain_exec_success(_):
    _CLIENT_MANAGER.configured = True
    expected_result = "final_output_data"
    mock_client = MagicMock()
    mock_client.pyfaas_chain_exec.return_value = {
        "status": "ok",
        "result": expected_result,
        "message": ""
    }
    _CLIENT_MANAGER.client = mock_client

    with patch("pyfaas.pyfaas.logger") as mock_logger:
        result = pyfaas_chain_exec(VALID_WORKFLOW)

    assert result == expected_result
    mock_client.pyfaas_chain_exec.assert_called_once_with(VALID_WORKFLOW)
    mock_logger.info.assert_called()

@patch("pyfaas.pyfaas.validate_json_workflow_structure", side_effect=mock_validate_workflow_success)
def test_chain_exec_director_error(_):
    _CLIENT_MANAGER.configured = True
    error_message = "A task failed during execution"
    mock_client = MagicMock()
    mock_client.pyfaas_chain_exec.return_value = {
        "status": "error",
        "result": None,
        "message": error_message
    }
    _CLIENT_MANAGER.client = mock_client

    with patch("pyfaas.pyfaas.logger"):
        with pytest.raises(PyFaaSChainedExecutionError):
            pyfaas_chain_exec(VALID_WORKFLOW)

@patch("pyfaas.pyfaas.validate_json_workflow_structure", side_effect=mock_validate_workflow_success)
def test_chain_exec_timeout(_):
    _CLIENT_MANAGER.configured = True
    mock_client = MagicMock()
    mock_client.pyfaas_chain_exec.side_effect = zmq.Again()
    _CLIENT_MANAGER.client = mock_client

    with pytest.raises(PyFaaSTimeoutError):
        pyfaas_chain_exec(VALID_WORKFLOW)

@patch("pyfaas.pyfaas.validate_json_workflow_structure", side_effect=mock_validate_workflow_success)
def test_chain_exec_no_action(_):
    _CLIENT_MANAGER.configured = True
    expected_result = "unexpected_status_result"
    mock_client = MagicMock()
    # Simulate an unexpected success status (not 'ok', but not 'error' either)
    mock_client.pyfaas_chain_exec.return_value = {
        "status": "pending", # Not 'ok'
        "result": expected_result,
        "message": ""
    }
    _CLIENT_MANAGER.client = mock_client

    # Since status != 'ok', it should fall to the 'else' block and raise PyFaaSChainedExecutionError
    with patch("pyfaas.pyfaas.logger"):
        with pytest.raises(PyFaaSChainedExecutionError):
            pyfaas_chain_exec(VALID_WORKFLOW)
