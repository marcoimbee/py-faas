import pytest
from unittest.mock import MagicMock, patch
import zmq

from pyfaas.pyfaas import pyfaas_ping, _CLIENT_MANAGER
from pyfaas.exceptions import (
    PyFaaSTimeoutError,
    PyFaaSPingingError
)


def test_ping_not_configured():
    _CLIENT_MANAGER.configured = False

    with pytest.raises(RuntimeError) as excinfo:
        pyfaas_ping()

    assert 'PyFaaS has not been configured' in str(excinfo.value)
    
def test_ping_success():
    _CLIENT_MANAGER.configured = True
    mock_client = MagicMock()
    mock_client.pyfaas_ping.return_value = {
        "status": "ok",
        "result": "PONG",
        "message": ""
    }
    _CLIENT_MANAGER.client = mock_client

    with patch("pyfaas.pyfaas.logger") as mock_logger:
        pyfaas_ping()

    mock_client.pyfaas_ping.assert_called_once()
    mock_logger.info.assert_called_once_with("Worker says: 'PONG'")

def test_ping_timeout():
    _CLIENT_MANAGER.configured = True
    mock_client = MagicMock()
    mock_client.pyfaas_ping.side_effect = zmq.Again()
    _CLIENT_MANAGER.client = mock_client

    with pytest.raises(PyFaaSTimeoutError) as excinfo:
        pyfaas_ping()
        
    assert 'Timeout while waiting for Director\'s response' in str(excinfo.value)

def test_ping_director_error():
    _CLIENT_MANAGER.configured = True
    error_message = "Worker is unavailable or broken"
    mock_client = MagicMock()
    mock_client.pyfaas_ping.return_value = {
        "status": "error",
        "result": None,
        "message": error_message
    }
    _CLIENT_MANAGER.client = mock_client

    with patch("pyfaas.pyfaas.logger") as mock_logger:
        with pytest.raises(PyFaaSPingingError) as excinfo:
            pyfaas_ping()

    assert error_message in str(excinfo.value)
    mock_logger.warning.assert_called_once_with(f'Error while PING-ing the worker: {error_message}')
    
def test_ping_unexpected_status():
    _CLIENT_MANAGER.configured = True
    unexpected_status = "unknown"
    unexpected_message = "Strange response from worker"
    mock_client = MagicMock()
    mock_client.pyfaas_ping.return_value = {
        "status": unexpected_status,
        "result": None,
        "message": unexpected_message
    }
    _CLIENT_MANAGER.client = mock_client

    with patch("pyfaas.pyfaas.logger") as mock_logger:
        with pytest.raises(PyFaaSPingingError) as excinfo:
            pyfaas_ping()

    assert unexpected_message in str(excinfo.value)
    mock_logger.warning.assert_called_once_with(f'Error while PING-ing the worker: {unexpected_message}')
