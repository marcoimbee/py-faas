import base64
import json
from pathlib import Path
from unittest.mock import MagicMock

import dill
import pytest

from pyfaas_worker.app.pyfaas_worker import PyfaasWorker


def add(a: int, b: int) -> int:
    return a + b


@pytest.fixture
def worker(worker_config):
    w = PyfaasWorker(worker_config)
    real_socket, real_context = w._zmq_socket, w._zmq_context
    w._zmq_socket = MagicMock()  # never actually bind/send over a real socket
    yield w
    # __init__ always opens a real zmq context/socket regardless of what a given test
    # needs -- leaving many of these unclosed across a test session can hang later,
    # unrelated tests that create their own zmq context (observed empirically). Close
    # the originals captured above, since a test may itself have replaced
    # w._zmq_context/w._zmq_socket with a mock by the time this teardown runs.
    real_socket.close(linger=0)
    real_context.term()


# --- known-bug regression: a mid-sync unregister can hang the Director's synchronization thread forever ---

def test_forward_function_code_for_unknown_func_id_should_not_raise(worker):
    # The Director's synchronization thread blocks on a queue.get() waiting for
    # this response. If the requesting client unregisters the function between
    # the Director's request and this call, self._functions[requested_func_id]
    # raises an uncaught KeyError here, no response is ever queued, and the
    # Director's synchronization thread hangs permanently (requires a restart).
    try:
        worker._forward_function_code_and_registering_client({'func_id': 'never-registered'})
    except KeyError:
        pytest.xfail('_forward_function_code_and_registering_client raises KeyError for an unregistered '
                      'func_id instead of sending some response, which hangs the Director\'s '
                      'synchronization thread forever')

    assert worker._outgoing_tx_queue.qsize() == 1


# --- state persistence: restore on init ---

def test_worker_restores_state_from_dump_file_on_init(worker_config, tmp_path):
    state = {
        'functions': {'fid1': {'name': 'add', 'code': add, 'registering_client': 'client-1'}},
        'stats': {'fid1': {}},
        'request_count': 3,
    }
    with open(tmp_path / 'worker_dump.bin', 'wb') as f:
        dill.dump(state, f)
    worker_config['behavior']['shutdown_persistence'] = True

    w = PyfaasWorker(worker_config)
    try:
        assert w._functions['fid1']['name'] == 'add'
        assert w._request_count == 3
    finally:
        # __init__ always opens a real zmq context/socket -- close it rather than
        # leaking it, which has been observed to hang later, unrelated tests.
        w._zmq_socket.close(linger=0)
        w._zmq_context.term()


def test_worker_load_failure_falls_back_to_empty_state(worker_config, tmp_path):
    (tmp_path / 'worker_dump.bin').write_bytes(b'not a valid dill pickle')
    worker_config['behavior']['shutdown_persistence'] = True

    w = PyfaasWorker(worker_config)
    try:
        assert w._functions == {}
        assert w._stats == {}
        assert w._request_count == 0
    finally:
        w._zmq_socket.close(linger=0)
        w._zmq_context.term()


# --- _dump_worker_state / _load_worker_state round trip ---

def test_dump_and_load_worker_state_round_trip(worker, tmp_path):
    worker._dump_file = str(tmp_path / 'dump.bin')
    worker._functions = {'fid1': {'name': 'add', 'code': add, 'registering_client': 'client-1'}}
    worker._stats = {'fid1': {'#calls': 1}}
    worker._request_count = 5

    worker._dump_worker_state()
    loaded = worker._load_worker_state()

    assert loaded['functions']['fid1']['name'] == 'add'
    assert loaded['stats'] == {'fid1': {'#calls': 1}}
    assert loaded['request_count'] == 5


def test_load_worker_state_wraps_io_errors(worker, tmp_path):
    worker._dump_file = str(tmp_path / 'does_not_exist.bin')
    with pytest.raises(Exception):
        worker._load_worker_state()


# --- _cleanup: dump-on-shutdown ---

def test_cleanup_dumps_state_when_persistence_enabled_and_requests_processed(worker, tmp_path):
    worker._config['behavior']['shutdown_persistence'] = True
    worker._dump_file = str(tmp_path / 'dump.bin')
    worker._request_count = 1

    worker._cleanup()

    assert Path(worker._dump_file).exists()


def test_cleanup_skips_dump_when_no_requests_processed(worker, tmp_path):
    worker._config['behavior']['shutdown_persistence'] = True
    worker._dump_file = str(tmp_path / 'dump.bin')
    worker._request_count = 0

    worker._cleanup()

    assert not Path(worker._dump_file).exists()


# --- _register_to_director: no ACK within time limit kills the worker ---

def test_register_to_director_no_ack_kills_worker(worker, monkeypatch):
    class FakePoller:
        def register(self, socket, flags):
            pass

        def poll(self, timeout=None):
            return {}  # no sockets ready -> no ACK received

    monkeypatch.setattr('pyfaas_worker.app.pyfaas_worker.zmq.Poller', FakePoller)
    worker._zmq_context = MagicMock()  # the real context's original (now-orphaned) socket
    # would otherwise make a real ctx.term() inside _kill_worker block forever

    with pytest.raises(SystemExit):
        worker._register_to_director()


# --- _synchronize_state ---

def test_synchronize_state_advertises_functions_and_installs_missing_ones(worker):
    worker._functions = {'fid_existing': {'name': 'existing', 'code': add, 'registering_client': 'client-1'}}
    serialized = base64.b64encode(dill.dumps(add)).decode('utf-8')
    worker._incoming_sync_function_code_queue.put({'missing_functions_total': 1})
    worker._incoming_sync_function_code_queue.put({
        'func_id': 'fid_new', 'registering_client': 'client-2', 'serialized_func_base64': serialized,
    })

    worker._synchronize_state()

    assert worker._functions['fid_new']['name'] == 'add'
    assert worker._functions['fid_new']['registering_client'] == 'client-2'
    sent = worker._outgoing_tx_queue.get_nowait()
    sent_payload = json.loads(sent[1].decode())
    assert sent_payload['action'] == 'current_functions_state'
    assert sent_payload['functions'] == ['fid_existing']


# --- _handle_incoming_request: unknown command ---

def test_handle_incoming_request_unknown_command_is_logged_not_raised(worker):
    worker._file_logger = MagicMock()
    worker._handle_incoming_request('unknown_command', {})  # must not raise
    worker._file_logger.log.assert_called_once()
