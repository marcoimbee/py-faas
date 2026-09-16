import pytest


@pytest.fixture
def director_config(tmp_path):
    return {
        'network': {'director_ip_addr': '127.0.0.1', 'director_port': 40000},
        'misc': {'greeting_msg': 'hi'},
        'logging': {
            'log_level': 'debug',
            'log_directory': str(tmp_path / 'director_logs'),
            'log_filename': 'director_log.log',
        },
        'statistics': {'enabled': True},
        'workers': {
            'heartbeat_check_interval_ms': 2000,
            'expected_heartbeat_interval_ms': 2000,
            'worker_selection_strategy': 'Round-Robin',
            'synchronization_interval_ms': 5000,
        },
    }


@pytest.fixture
def worker_config(tmp_path):
    return {
        'network': {
            'director_ip_addr': '127.0.0.1',
            'director_port': 40000,
            'heartbeat_interval_ms': 2000,
        },
        'misc': {'greeting_msg': 'hi'},
        'behavior': {
            'dump_file': str(tmp_path / 'worker_dump.bin'),
            'shutdown_persistence': False,
            'caching': {'policy': 'LRU', 'max_size': 10},
        },
        'logging': {
            'log_level': 'debug',
            'log_directory': str(tmp_path / 'worker_logs'),
            'log_filename': 'worker_log.log',
        },
        'statistics': {'enabled': True},
    }
