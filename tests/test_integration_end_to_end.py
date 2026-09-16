"""
Real end-to-end smoke test: a live Director and Worker talking over actual
ZeroMQ TCP sockets, driven through the real PyfaasClient wire protocol.
Unlike the rest of the suite (which mocks the network to test logic in
isolation), this catches wire-format/threading bugs that only show up when
the three processes' pieces actually talk to each other.
"""
import threading
import time

import pytest

from pyfaas.pyfaas_client.pyfaas_client import PyfaasClient
from pyfaas_director.app.pyfaas_director import PyfaasDirector
from pyfaas_worker.app.pyfaas_worker import PyfaasWorker

_PORT = 41055


def add(a: int, b: int) -> int:
    return a + b


@pytest.fixture(scope='module')
def cluster(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp('pyfaas_e2e')

    director_config = {
        'network': {'director_ip_addr': '127.0.0.1', 'director_port': _PORT},
        'misc': {'greeting_msg': 'hi'},
        'logging': {'log_level': 'warning', 'log_directory': str(tmp_path / 'director_logs'), 'log_filename': 'd.log'},
        'statistics': {'enabled': True},
        'workers': {
            'heartbeat_check_interval_ms': 10000,
            'expected_heartbeat_interval_ms': 10000,
            'worker_selection_strategy': 'Round-Robin',
            'synchronization_interval_ms': 60000,
        },
    }
    worker_config = {
        'network': {'director_ip_addr': '127.0.0.1', 'director_port': _PORT, 'heartbeat_interval_ms': 2000},
        'misc': {'greeting_msg': 'hi'},
        'behavior': {
            'dump_file': str(tmp_path / 'dump.bin'),
            'shutdown_persistence': False,
            'caching': {'policy': 'LRU', 'max_size': 10},
        },
        'logging': {'log_level': 'warning', 'log_directory': str(tmp_path / 'worker_logs'), 'log_filename': 'w.log'},
        'statistics': {'enabled': True},
    }

    director = PyfaasDirector(director_config)
    threading.Thread(target=director.run, daemon=True).start()
    time.sleep(0.3)  # let the ROUTER socket bind

    worker = PyfaasWorker(worker_config)
    threading.Thread(target=worker.run, daemon=True).start()

    deadline = time.time() + 10
    while time.time() < deadline and len(director._workers) < 1:
        time.sleep(0.1)
    assert len(director._workers) == 1, 'worker never registered with the director'

    client = PyfaasClient('127.0.0.1', _PORT, receive_timeout_s=5)
    yield client
    client.zmq_close()


def test_full_register_exec_list_stats_unregister_roundtrip(cluster):
    client = cluster

    register_resp = client.pyfaas_register(add)
    assert register_resp['status'] == 'ok'
    func_id = register_resp['result']

    ping_resp = client.pyfaas_ping()
    assert ping_resp['status'] == 'ok'
    assert ping_resp['result'] == 'PONG'

    # NOTE: default_args={} must be passed explicitly here. PyfaasClient.pyfaas_exec()'s
    # own default is None, which the worker cannot handle (see
    # test_exec_none_default_args_crashes in test_worker_operations.py) -- only the
    # higher-level pyfaas.pyfaas_exec() wrapper normalizes None to {} before sending.
    exec_resp = client.pyfaas_exec(func_id, [2, 3], {})
    assert exec_resp['status'] == 'ok'
    assert exec_resp['result'] == 5

    list_resp = client.pyfaas_list()
    assert list_resp['status'] == 'ok'
    assert list_resp['result'] == {func_id: 'add'}

    stats_resp = client.pyfaas_get_stats()
    assert stats_resp['status'] == 'ok'
    assert stats_resp['result'][func_id]['#calls'] == 1

    unregister_resp = client.pyfaas_unregister(func_id)
    assert unregister_resp['status'] == 'ok'
    assert unregister_resp['action'] == 'unregistered'

    # NOTE: deliberately not exec-ing func_id again here. Doing so crashes the Director's
    # I/O thread (KeyError in _select_worker, since unregister already deleted the
    # func_id -> worker mapping) -- see test_exec_after_unregister_crashes_director in
    # test_director_core.py for a focused regression test of that bug.
