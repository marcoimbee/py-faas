import logging
from pathlib import Path

import pytest

from pyfaas_director.app.exceptions import DirectorConfigError
from pyfaas_director.app.util.general import read_config_toml, setup_logging

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _valid_toml(**overrides):
    values = {
        'director_ip_addr': '127.0.0.1',
        'director_port': 40000,
        'heartbeat_check_interval_ms': 2000,
        'expected_heartbeat_interval_ms': 2000,
        'synchronization_interval_ms': 5000,
        'worker_selection_strategy': 'Round-Robin',
    }
    values.update(overrides)
    return f"""
[network]
director_ip_addr = "{values['director_ip_addr']}"
director_port = {values['director_port']}

[workers]
heartbeat_check_interval_ms = {values['heartbeat_check_interval_ms']}
expected_heartbeat_interval_ms = {values['expected_heartbeat_interval_ms']}
synchronization_interval_ms = {values['synchronization_interval_ms']}
worker_selection_strategy = "{values['worker_selection_strategy']}"
"""


def _write(tmp_path, body):
    path = tmp_path / 'config.toml'
    path.write_text(body)
    return str(path)


def test_valid_config_loads(tmp_path):
    config = read_config_toml(_write(tmp_path, _valid_toml()))
    assert config['workers']['worker_selection_strategy'] == 'Round-Robin'


def test_invalid_ip_raises(tmp_path):
    with pytest.raises(DirectorConfigError):
        read_config_toml(_write(tmp_path, _valid_toml(director_ip_addr='bad-ip')))


@pytest.mark.parametrize('port', [1024, 65535, 70000])
def test_invalid_port_raises(tmp_path, port):
    with pytest.raises(DirectorConfigError):
        read_config_toml(_write(tmp_path, _valid_toml(director_port=port)))


@pytest.mark.parametrize('field', [
    'heartbeat_check_interval_ms',
    'expected_heartbeat_interval_ms',
    'synchronization_interval_ms',
])
def test_non_positive_interval_raises(tmp_path, field):
    with pytest.raises(DirectorConfigError):
        read_config_toml(_write(tmp_path, _valid_toml(**{field: 0})))


def test_unknown_worker_selection_strategy_raises(tmp_path):
    with pytest.raises(DirectorConfigError):
        read_config_toml(_write(tmp_path, _valid_toml(worker_selection_strategy='Fastest')))


def test_shipped_director_config_loads():
    config = read_config_toml(str(_REPO_ROOT / 'src' / 'pyfaas_director' / 'director_config.toml'))
    assert config['workers']['worker_selection_strategy'] == 'Round-Robin'


@pytest.mark.parametrize('level, expected', [
    ('debug', logging.DEBUG),
    ('info', logging.INFO),
    ('warning', logging.WARNING),
    ('error', logging.ERROR),
    ('critical', logging.CRITICAL),
    ('fatal', logging.FATAL),
    ('unknown-level', logging.INFO),  # falls back to INFO
])
def test_setup_logging_sets_root_level(level, expected):
    setup_logging(level)
    assert logging.getLogger().level == expected
