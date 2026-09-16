import logging
from pathlib import Path

import pytest

import pyfaas.pyfaas as pyfaas
from pyfaas.util.general import read_config_toml, setup_logging

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _write_toml(tmp_path, body):
    path = tmp_path / 'config.toml'
    path.write_text(body)
    return str(path)


def _valid_toml(**overrides):
    values = {
        'director_ip_addr': '127.0.0.1',
        'director_port': 40000,
        'receive_timeout_s': 5,
        'log_level': 'info',
    }
    values.update(overrides)
    return f"""
[network]
director_ip_addr = "{values['director_ip_addr']}"
director_port = {values['director_port']}
receive_timeout_s = {values['receive_timeout_s']}

[misc]
log_level = "{values['log_level']}"
"""


def test_valid_config_loads(tmp_path):
    config = read_config_toml(_write_toml(tmp_path, _valid_toml()))
    assert config['network']['director_ip_addr'] == '127.0.0.1'
    assert config['network']['director_port'] == 40000


@pytest.mark.parametrize('port', [0, 1024, 65535, 70000])
def test_invalid_port_raises(tmp_path, port):
    with pytest.raises(Exception):
        read_config_toml(_write_toml(tmp_path, _valid_toml(director_port=port)))


def test_invalid_ip_raises(tmp_path):
    with pytest.raises(Exception):
        read_config_toml(_write_toml(tmp_path, _valid_toml(director_ip_addr='not-an-ip')))


def test_negative_receive_timeout_raises(tmp_path):
    with pytest.raises(Exception):
        read_config_toml(_write_toml(tmp_path, _valid_toml(receive_timeout_s=-1)))


def test_non_int_receive_timeout_raises(tmp_path):
    path = tmp_path / 'config.toml'
    path.write_text("""
[network]
director_ip_addr = "127.0.0.1"
director_port = 40000
receive_timeout_s = "5"

[misc]
log_level = "info"
""")
    with pytest.raises(Exception):
        read_config_toml(str(path))


@pytest.mark.xfail(reason="pyfaas.pyfaas._DEFAULT_CONFIG_FILE_PATH points at 'test/client_config.toml', but "
                           "neither that file nor a 'test/' directory exists anywhere in the repo, so "
                           "pyfaas_config() with no file_path always fails with PyFaaSConfigError")
def test_default_client_config_path_exists():
    default_path = _REPO_ROOT / pyfaas._DEFAULT_CONFIG_FILE_PATH
    assert default_path.exists()


@pytest.mark.parametrize('level, expected', [
    ('debug', logging.DEBUG),
    ('info', logging.INFO),
    ('warning', logging.WARNING),
    ('error', logging.ERROR),
    ('critical', logging.CRITICAL),
    ('unknown-level', logging.INFO),  # falls back to INFO
])
def test_setup_logging_sets_root_level(level, expected):
    setup_logging(level)
    assert logging.getLogger().level == expected
