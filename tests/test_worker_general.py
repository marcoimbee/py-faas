from pathlib import Path

import pytest

from pyfaas_worker.app.exceptions import WorkerConfigError
from pyfaas_worker.app.util.general import read_config_toml

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _valid_toml(**overrides):
    values = {
        'director_ip_addr': '127.0.0.1',
        'director_port': 40000,
        'heartbeat_interval_ms': 2000,
        'caching_policy': 'LRU',
        'caching_max_size': 10,
        'shutdown_persistence': 'false',
        'dump_file': '"dump.bin"',
    }
    values.update(overrides)
    return f"""
[network]
director_ip_addr = "{values['director_ip_addr']}"
director_port = {values['director_port']}
heartbeat_interval_ms = {values['heartbeat_interval_ms']}

[behavior]
dump_file = {values['dump_file']}
shutdown_persistence = {values['shutdown_persistence']}

[behavior.caching]
policy = "{values['caching_policy']}"
max_size = {values['caching_max_size']}
"""


def _write(tmp_path, body):
    path = tmp_path / 'config.toml'
    path.write_text(body)
    return str(path)


def test_valid_config_loads(tmp_path):
    config = read_config_toml(_write(tmp_path, _valid_toml()))
    assert config['behavior']['caching']['policy'] == 'LRU'


def test_invalid_ip_raises(tmp_path):
    with pytest.raises(WorkerConfigError):
        read_config_toml(_write(tmp_path, _valid_toml(director_ip_addr='bad-ip')))


@pytest.mark.parametrize('port', [1024, 65535, 70000])
def test_invalid_port_raises(tmp_path, port):
    with pytest.raises(WorkerConfigError):
        read_config_toml(_write(tmp_path, _valid_toml(director_port=port)))


def test_unknown_caching_policy_raises(tmp_path):
    with pytest.raises(WorkerConfigError):
        read_config_toml(_write(tmp_path, _valid_toml(caching_policy='LFU')))


def test_negative_cache_max_size_raises(tmp_path):
    with pytest.raises(WorkerConfigError):
        read_config_toml(_write(tmp_path, _valid_toml(caching_max_size=-1)))


def test_non_positive_heartbeat_interval_raises(tmp_path):
    with pytest.raises(WorkerConfigError):
        read_config_toml(_write(tmp_path, _valid_toml(heartbeat_interval_ms=0)))


def test_shipped_worker_config_loads():
    config = read_config_toml(str(_REPO_ROOT / 'src' / 'pyfaas_worker' / 'worker_config.toml'))
    assert config['behavior']['caching']['policy'] == 'LRU'
