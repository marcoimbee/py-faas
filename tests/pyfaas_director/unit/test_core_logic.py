import pytest

from unittest.mock import MagicMock, patch
from pyfaas_director.app.pyfaas_director import PyfaasDirector


@pytest.fixture
def dummy_config():
    return {
        'network': {
            'director_ip_addr': '127.0.0.1',
            'director_port': 5555
        },
        'logging': {
            'log_level': 'debug',
            'log_directory': '/tmp',
            'log_filename': 'test.log'
        },
        'statistics': {
            'enabled': True
        },
        'workers': {
            'heartbeat_check_interval_ms': 2000,
            'expected_heartbeat_interval_ms': 2000,
            'worker_selection_strategy': 'Round-Robin',
            'synchronization_interval_ms': 5000
        },
        'misc': {
            'greeting_msg': 'Hello brother'
        }
    }

@patch('pyfaas_director.app.pyfaas_director.zmq.Context')
@patch('pyfaas_director.app.pyfaas_director.FileLogger')
def test_compute_function_id(mock_zmq_context, mock_file_logger, dummy_config):
    director = PyfaasDirector(dummy_config)
    func_name = 'test_func'
    func_code = 'print("hello")'
    func_id = director._compute_function_id(func_name, func_code)
    
    assert isinstance(func_id, str)
    assert len(func_id) == 64  # SHA256 hex digest length
