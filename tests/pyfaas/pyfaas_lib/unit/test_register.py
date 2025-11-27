import pytest
from unittest.mock import MagicMock, patch
import zmq

from pyfaas.pyfaas import pyfaas_register, _CLIENT_MANAGER
from pyfaas.exceptions import (
    PyFaaSTimeoutError,
    PyFaaSFunctionRegistrationError
)


def sample_func(x: int) -> int:
    return x

def test_register_not_configured():
    _CLIENT_MANAGER.configured = False

    with pytest.raises(RuntimeError):
        pyfaas_register(lambda x: x)

def test_register_missing_function():
    _CLIENT_MANAGER.configured = True

    with pytest.raises(PyFaaSFunctionRegistrationError):
        pyfaas_register(None)

def test_register_success():
    _CLIENT_MANAGER.configured = True
    mock_client = MagicMock()
    mock_client.pyfaas_register.return_value = {
        "status": "ok",
        "action": "registered",
        "result": "func123",
        "message": ""
    }
    _CLIENT_MANAGER.client = mock_client

    with patch("pyfaas.pyfaas.logger") as mock_logger:
        fid = pyfaas_register(sample_func)

    assert fid == "func123"
    mock_client.pyfaas_register.assert_called_once_with(sample_func)
    mock_logger.info.assert_called()

def test_register_no_action():
    _CLIENT_MANAGER.configured = True
    mock_client = MagicMock()
    mock_client.pyfaas_register.return_value = {
        "status": "ok",
        "action": "no_action",
        "result": "func123",
        "message": ""
    }
    _CLIENT_MANAGER.client = mock_client

    with patch("pyfaas.pyfaas.logger") as mock_logger:
        fid = pyfaas_register(sample_func)

    assert fid == "func123"
    mock_logger.info.assert_called()

def test_register_director_error():
    _CLIENT_MANAGER.configured = True
    mock_client = MagicMock()
    mock_client.pyfaas_register.return_value = {
        "status": "error",
        "action": "",
        "result": None,
        "message": "bad function"
    }
    _CLIENT_MANAGER.client = mock_client

    with patch("pyfaas.pyfaas.logger"):
        with pytest.raises(PyFaaSFunctionRegistrationError):
            pyfaas_register(sample_func)

def test_register_timeout():
    _CLIENT_MANAGER.configured = True
    mock_client = MagicMock()
    mock_client.pyfaas_register.side_effect = zmq.Again()
    _CLIENT_MANAGER.client = mock_client

    with pytest.raises(PyFaaSTimeoutError):
        pyfaas_register(sample_func)

def test_register_missing_annotations():
    _CLIENT_MANAGER.configured = True

    def bad_func(x):
        return x  # No annotation

    mock_client = MagicMock()
    mock_client.pyfaas_register.return_value = {
        "status": "error",
        "message": "Missing type annotations"
    }
    _CLIENT_MANAGER.client = mock_client

    with pytest.raises(PyFaaSFunctionRegistrationError):
        pyfaas_register(bad_func)
