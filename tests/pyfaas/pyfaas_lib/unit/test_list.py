import pytest
from unittest.mock import MagicMock
import zmq

from pyfaas.pyfaas import pyfaas_list, _CLIENT_MANAGER
from pyfaas.exceptions import (
    PyFaaSTimeoutError,
    PyFaaSFunctionListingError,
)


@pytest.fixture(autouse=True)
def reset_manager():
    """Reset global client manager before each test."""
    _CLIENT_MANAGER.client = None
    _CLIENT_MANAGER.configured = False
    yield
    _CLIENT_MANAGER.client = None
    _CLIENT_MANAGER.configured = False


def test_list_not_configured():
    with pytest.raises(RuntimeError):
        pyfaas_list()


def test_list_timeout():
    _CLIENT_MANAGER.configured = True

    mock_client = MagicMock()
    mock_client.pyfaas_list.side_effect = zmq.Again()
    _CLIENT_MANAGER.client = mock_client

    with pytest.raises(PyFaaSTimeoutError):
        pyfaas_list()


def test_list_success():
    _CLIENT_MANAGER.configured = True

    sample_list = {
        "functions": [
            {"id": "abc", "name": "foo"},
            {"id": "def", "name": "bar"}
        ]
    }

    mock_client = MagicMock()
    mock_client.pyfaas_list.return_value = {
        "status": "ok",
        "result": sample_list,
        "message": ""
    }
    _CLIENT_MANAGER.client = mock_client

    res = pyfaas_list()

    assert res == sample_list
    mock_client.pyfaas_list.assert_called_once()


def test_list_director_error():
    _CLIENT_MANAGER.configured = True

    mock_client = MagicMock()
    mock_client.pyfaas_list.return_value = {
        "status": "err",
        "result": None,
        "message": "Something went wrong!"
    }
    _CLIENT_MANAGER.client = mock_client

    with pytest.raises(PyFaaSFunctionListingError):
        pyfaas_list()
