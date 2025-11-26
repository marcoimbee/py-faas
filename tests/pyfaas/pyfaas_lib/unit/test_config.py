import pytest
import pyfaas

from unittest.mock import MagicMock, patch

import pyfaas.pyfaas as pyfaas_mod
from pyfaas.exceptions import PyFaaSConfigError


fake_config = {
        "network": {
            "director_ip_addr": "1.2.3.4",
            "director_port": 9999,
            "receive_timeout_s": 5,
        },
        "misc": {"log_level": "debug"},
    }

@pytest.fixture(autouse=True)
def reset_manager_state():
    """reset global state before running each test."""
    pyfaas_mod._CLIENT_MANAGER.configured = False
    pyfaas_mod._CLIENT_MANAGER.config = None
    pyfaas_mod._CLIENT_MANAGER.client = None
    yield

def test_use_provided_config_file_path():
    with patch("pyfaas.pyfaas.read_config_toml", return_value=fake_config) as mock_read, \
         patch("pyfaas.pyfaas.setup_logging") as mock_log, \
         patch("pyfaas.pyfaas_client.pyfaas_client.PyfaasClient") as mock_client:

        pyfaas_mod.pyfaas_config("test_config.toml")

        mock_read.assert_called_once_with("test_config.toml")
        assert pyfaas_mod._CONFIG_FILE_PATH == "test_config.toml"

def test_use_default_config_file_path_when_none_provided():
    with patch("pyfaas.pyfaas.read_config_toml", return_value=fake_config) as mock_read, \
         patch("pyfaas.pyfaas.setup_logging") as mock_log, \
         patch("pyfaas.pyfaas_client.pyfaas_client.PyfaasClient") as mock_client:

        pyfaas_mod.pyfaas_config()

        mock_read.assert_called_once_with(pyfaas_mod._DEFAULT_CONFIG_FILE_PATH)
        assert pyfaas_mod._CONFIG_FILE_PATH == pyfaas_mod._DEFAULT_CONFIG_FILE_PATH

def test_sets_config_in_manager():
    with patch("pyfaas.pyfaas.read_config_toml", return_value=fake_config), \
         patch("pyfaas.pyfaas.setup_logging"), \
         patch("pyfaas.pyfaas_client.pyfaas_client.PyfaasClient"):

        pyfaas_mod.pyfaas_config("x.toml")

        assert pyfaas_mod._CLIENT_MANAGER.config == fake_config
        assert pyfaas_mod._CLIENT_MANAGER.configured is True

def test_setup_logging_called_with_config_log_level():
    with patch("pyfaas.pyfaas.read_config_toml", return_value=fake_config), \
         patch("pyfaas.pyfaas_client.pyfaas_client.PyfaasClient"), \
         patch("pyfaas.pyfaas.setup_logging") as mock_log:

        pyfaas_mod.pyfaas_config("x.toml")
        mock_log.assert_called_once_with("debug")


def test_client_is_instantiated_correctly():
    with patch("pyfaas.pyfaas.read_config_toml", return_value=fake_config), \
         patch("pyfaas.pyfaas.setup_logging"), \
         patch("pyfaas.pyfaas_client.pyfaas_client.PyfaasClient") as mock_client:

        pyfaas_mod.pyfaas_config("x.toml")

        mock_client.assert_called_once_with("1.2.3.4", 9999, 5)
        assert pyfaas_mod._CLIENT_MANAGER.client is mock_client.return_value


def test_does_not_reconfigure_if_already_configured():
    pyfaas_mod._CLIENT_MANAGER.configured = True

    with patch("pyfaas.pyfaas.read_config_toml") as mock_read, \
         patch("pyfaas.pyfaas.setup_logging") as mock_log, \
         patch("pyfaas.pyfaas_client.pyfaas_client.PyfaasClient") as mock_client:

        pyfaas_mod.pyfaas_config("x.toml")

        mock_read.assert_not_called()
        mock_log.assert_not_called()
        mock_client.assert_not_called()


def test_raises_on_config_parse_error():
    with patch("pyfaas.pyfaas.read_config_toml", side_effect=ValueError("bad toml")), \
         patch("pyfaas.pyfaas.setup_logging"), \
         patch("pyfaas.pyfaas_client.pyfaas_client.PyfaasClient"):

        with pytest.raises(PyFaaSConfigError):
            pyfaas_mod.pyfaas_config("broken.toml")


def test_logs_warning_when_no_path_provided(caplog):
    with patch("pyfaas.pyfaas.read_config_toml", return_value=fake_config), \
         patch("pyfaas.pyfaas_client.pyfaas_client.PyfaasClient"), \
         patch("pyfaas.pyfaas.setup_logging"):

        pyfaas_mod.pyfaas_config()

        assert any("Unspecified PyFaaS configuration file path" in m for m in caplog.text.splitlines())


def test_stores_client_in_manager():
    with patch("pyfaas.pyfaas.read_config_toml", return_value=fake_config), \
         patch("pyfaas.pyfaas.setup_logging"), \
         patch("pyfaas.pyfaas_client.pyfaas_client.PyfaasClient") as mock_client:

        pyfaas_mod.pyfaas_config("file.toml")

        assert pyfaas_mod._CLIENT_MANAGER.client is mock_client.return_value
