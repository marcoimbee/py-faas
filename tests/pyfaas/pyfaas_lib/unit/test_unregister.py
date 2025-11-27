import pytest
from unittest.mock import MagicMock, patch
import zmq

from pyfaas.pyfaas import pyfaas_unregister, _CLIENT_MANAGER
from pyfaas.exceptions import (
    PyFaaSFunctionUnregistrationError,
    PyFaaSTimeoutError,
)


FUNC_ID = "abc123"

def test_unregister_not_configured():
    _CLIENT_MANAGER.configured = False

    with pytest.raises(RuntimeError):
        pyfaas_unregister("abc123")

def test_unregister_missing_func_id():
    _CLIENT_MANAGER.configured = True

    with pytest.raises(PyFaaSFunctionUnregistrationError):
        pyfaas_unregister(None)

    with pytest.raises(PyFaaSFunctionUnregistrationError):
        pyfaas_unregister("")

def test_unregister_success():
    _CLIENT_MANAGER.configured = True

    mock_client = MagicMock()
    mock_client.pyfaas_unregister.return_value = {
        "status": "ok",
        "action": "unregistered",
        "message": ""
    }
    _CLIENT_MANAGER.client = mock_client

    with patch("pyfaas.pyfaas.logger") as mock_logger:
        result = pyfaas_unregister(FUNC_ID)

    assert result == 1
    mock_client.pyfaas_unregister.assert_called_once_with(FUNC_ID)
    mock_logger.info.assert_called_with(f"Successfully unregistered '{FUNC_ID}'")

def test_unregister_error():
    _CLIENT_MANAGER.configured = True

    mock_client = MagicMock()
    mock_client.pyfaas_unregister.return_value = {
        "status": "err",
        "action": "",
        "message": "not allowed"
    }
    _CLIENT_MANAGER.client = mock_client

    with patch("pyfaas.pyfaas.logger") as mock_logger:
        with pytest.raises(PyFaaSFunctionUnregistrationError):
            pyfaas_unregister(FUNC_ID)

    mock_logger.warning.assert_called()

def test_unregister_ok_wrong_action():
    _CLIENT_MANAGER.configured = True

    mock_client = MagicMock()
    mock_client.pyfaas_unregister.return_value = {
        "status": "ok",
        "action": "ignored",
        "message": ""
    }
    _CLIENT_MANAGER.client = mock_client

    with patch("pyfaas.pyfaas.logger"):
        result = pyfaas_unregister(FUNC_ID)

    assert result is None  # By design

def test_unregister_timeout():
    _CLIENT_MANAGER.configured = True

    mock_client = MagicMock()
    mock_client.pyfaas_unregister.side_effect = zmq.Again()
    _CLIENT_MANAGER.client = mock_client

    with pytest.raises(PyFaaSTimeoutError):
        pyfaas_unregister(FUNC_ID)
