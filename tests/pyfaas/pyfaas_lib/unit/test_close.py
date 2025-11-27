import pytest
from unittest.mock import MagicMock, patch

from pyfaas.pyfaas import pyfaas_close, _CLIENT_MANAGER


def test_pyfaas_close_with_client():
    mock_client = MagicMock()
    mock_client.zmq_close = MagicMock()

    # Setup client manager
    _CLIENT_MANAGER.client = mock_client
    _CLIENT_MANAGER.configured = True

    with patch("pyfaas.pyfaas.logger") as mock_logger:
        pyfaas_close()

    mock_client.zmq_close.assert_called_once()
    assert _CLIENT_MANAGER.client is None
    assert _CLIENT_MANAGER.configured is False
    mock_logger.info.assert_called_with("PyFaaS client session closed")

def test_pyfaas_close_without_client():
    # Setup state: no client
    _CLIENT_MANAGER.client = None
    _CLIENT_MANAGER.configured = True

    with patch("pyfaas.pyfaas.logger") as mock_logger:
        pyfaas_close()

    # zmq_close must NOT be called
    mock_logger.info.assert_not_called()
    assert _CLIENT_MANAGER.client is None
    # Configured must stay unchanged
    assert _CLIENT_MANAGER.configured is True

def test_pyfaas_close_client_manager_none():
    # Temporarily remove CLIENT_MANAGER
    from pyfaas import pyfaas
    original_manager = pyfaas._CLIENT_MANAGER
    pyfaas._CLIENT_MANAGER = None

    try:
        pyfaas_close()  # Should NOT crash
    finally:
        # Restore for other tests
        pyfaas._CLIENT_MANAGER = original_manager
