import pytest
from unittest.mock import MagicMock
import base64
import dill
import zmq

from pyfaas.pyfaas import pyfaas_exec, _CLIENT_MANAGER
from pyfaas.exceptions import (
    PyFaaSParameterMismatchError,
    PyFaaSTimeoutError,
    PyFaaSDeserializationError,
    PyFaaSFunctionExecutionError,
)


@pytest.fixture(autouse=True)
def reset_manager():
    """Ensure _CLIENT_MANAGER is reset before each test."""
    _CLIENT_MANAGER.client = None
    _CLIENT_MANAGER.configured = False
    yield
    _CLIENT_MANAGER.client = None
    _CLIENT_MANAGER.configured = False


def test_exec_not_configured():
    with pytest.raises(RuntimeError):
        pyfaas_exec("id123", [])


def test_exec_invalid_positional_arg_type():
    _CLIENT_MANAGER.configured = True
    _CLIENT_MANAGER.client = MagicMock()

    with pytest.raises(PyFaaSParameterMismatchError):
        pyfaas_exec("id123", "not_a_list")   # invalid type


def test_exec_timeout():
    _CLIENT_MANAGER.configured = True

    mock_client = MagicMock()
    mock_client.pyfaas_exec.side_effect = zmq.Again()
    _CLIENT_MANAGER.client = mock_client

    with pytest.raises(PyFaaSTimeoutError):
        pyfaas_exec("id123", [])


def test_exec_success_raw_json_result():
    _CLIENT_MANAGER.configured = True

    mock_client = MagicMock()
    mock_client.pyfaas_exec.return_value = {
        "status": "ok",
        "action": "executed",
        "result_type": "json",
        "result": {"value": 42},
        "message": "",
    }
    _CLIENT_MANAGER.client = mock_client

    res = pyfaas_exec("id123", [1, 2], {"x": 5}, save_in_cache=True)

    assert res == {"value": 42}
    mock_client.pyfaas_exec.assert_called_once_with("id123", [1, 2], {"x": 5}, True)


def test_exec_success_pickle_result():
    _CLIENT_MANAGER.configured = True

    original_obj = {"answer": 42}
    pickled = dill.dumps(original_obj)
    encoded = base64.b64encode(pickled).decode()

    mock_client = MagicMock()
    mock_client.pyfaas_exec.return_value = {
        "status": "ok",
        "action": "executed",
        "result_type": "pickle_base64",
        "result": encoded,
        "message": "",
    }
    _CLIENT_MANAGER.client = mock_client

    res = pyfaas_exec("id123", [])

    assert res == original_obj
    mock_client.pyfaas_exec.assert_called_once()


def test_exec_deserialization_failure():
    _CLIENT_MANAGER.configured = True

    mock_client = MagicMock()
    mock_client.pyfaas_exec.return_value = {
        "status": "ok",
        "action": "executed",
        "result_type": "pickle_base64",
        "result": "INVALID_BASE64_DATA",
        "message": "",
    }
    _CLIENT_MANAGER.client = mock_client

    with pytest.raises(PyFaaSDeserializationError):
        pyfaas_exec("id123", [])


def test_exec_error_from_director():
    _CLIENT_MANAGER.configured = True

    mock_client = MagicMock()
    mock_client.pyfaas_exec.return_value = {
        "status": "err",
        "action": "error",
        "result_type": None,
        "result": None,
        "message": "Function not found",
    }
    _CLIENT_MANAGER.client = mock_client

    with pytest.raises(PyFaaSFunctionExecutionError):
        pyfaas_exec("id123", [])


def test_exec_default_args_are_initialized():
    _CLIENT_MANAGER.configured = True

    mock_client = MagicMock()
    mock_client.pyfaas_exec.return_value = {
        "status": "ok",
        "action": "executed",
        "result_type": "json",
        "result": 123,
        "message": "",
    }
    _CLIENT_MANAGER.client = mock_client

    res = pyfaas_exec("id123", [1])

    assert res == 123
    mock_client.pyfaas_exec.assert_called_once_with("id123", [1], {}, False)
