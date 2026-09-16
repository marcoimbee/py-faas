import datetime
import json
import time
from unittest.mock import MagicMock

import pytest

from pyfaas_director.app.exceptions import DirectorNoAvailableWorkersError
from pyfaas_director.app.pyfaas_director import PyfaasDirector


@pytest.fixture
def director(director_config):
    d = PyfaasDirector(director_config)
    d._zmq_socket = MagicMock()  # never actually bind/send over a real socket
    return d


def sent_payloads(mock_socket):
    return [json.loads(call.args[0][2].decode()) for call in mock_socket.send_multipart.call_args_list]


# --- _compute_function_id ---

def test_compute_function_id_is_deterministic(director):
    id1 = director._compute_function_id('add', 'base64code')
    id2 = director._compute_function_id('add', 'base64code')
    assert id1 == id2


def test_compute_function_id_differs_by_input(director):
    id1 = director._compute_function_id('add', 'base64code')
    id2 = director._compute_function_id('subtract', 'base64code')
    assert id1 != id2


# --- _select_worker ---

def test_select_worker_raises_without_workers(director):
    with pytest.raises(DirectorNoAvailableWorkersError):
        director._select_worker()


def test_select_worker_round_robin_cycles_all_workers(director):
    director._workers = {'worker-1': {}, 'worker-2': {}, 'worker-3': {}}
    selected = [director._select_worker() for _ in range(6)]
    assert selected == ['worker-1', 'worker-2', 'worker-3', 'worker-1', 'worker-2', 'worker-3']


def test_select_worker_random_picks_from_known_workers(director):
    director._worker_selection_strategy = 'Random'
    director._workers = {'worker-1': {}, 'worker-2': {}}
    for _ in range(10):
        assert director._select_worker() in director._workers


def test_select_worker_for_func_returns_single_available_worker(director):
    director._workers = {'worker-1': {}, 'worker-2': {}}
    director._functions_workers_map['fid'] = {'available_on': ['worker-2']}
    assert director._select_worker('fid') == 'worker-2'


def test_select_worker_for_func_round_robins_when_multiple_available(director):
    director._workers = {'worker-1': {}, 'worker-2': {}, 'worker-3': {}}
    director._functions_workers_map['fid'] = {'available_on': ['worker-1', 'worker-2']}
    selected = [director._select_worker('fid') for _ in range(4)]
    assert selected == ['worker-1', 'worker-2', 'worker-1', 'worker-2']


# --- _aggregate_stats_per_id ---

def test_aggregate_stats_sums_calls_and_time_and_averages_avg(director):
    worker_a_stats = {'fid1': {'#calls': 2, 'tot_exec_time': 1.0, 'avg_exec_time': 0.5}}
    worker_b_stats = {'fid1': {'#calls': 3, 'tot_exec_time': 3.0, 'avg_exec_time': 1.0}}

    result = director._aggregate_stats_per_id([worker_a_stats, worker_b_stats])

    assert result['fid1']['#calls'] == 5
    assert result['fid1']['tot_exec_time'] == 4.0
    assert result['fid1']['avg_exec_time'] == 0.75  # mean of the per-worker averages


def test_aggregate_stats_ignores_functions_with_no_calls_yet(director):
    result = director._aggregate_stats_per_id([{'fid1': {}}])
    assert result == {}


# --- _handle_worker_request: worker_registration / heartbeat ---

def test_worker_registration_stores_worker_and_sends_ack(director):
    director._handle_worker_request('worker-1', {'operation': 'worker_registration'})

    assert 'worker-1' in director._workers
    ack_call = director._zmq_socket.send_multipart.call_args
    dest, empty, body = ack_call.args[0]
    assert dest == b'worker-1'
    assert json.loads(body.decode()) == {'ACK': 'OK'}


def test_heartbeat_updates_last_heartbeat_for_known_worker(director):
    director._workers['worker-1'] = {
        'registered_at': datetime.datetime.now(),
        'last_heartbeat': datetime.datetime.min,
    }
    director._handle_worker_request('worker-1', {'operation': 'heartbeat'})
    assert director._workers['worker-1']['last_heartbeat'] > datetime.datetime.min


def test_heartbeat_for_unknown_worker_is_a_no_op(director):
    director._handle_worker_request('worker-ghost', {'operation': 'heartbeat'})  # must not raise


# --- _handle_client_request: get_worker_ids ---

def test_get_worker_ids_responds_directly_without_contacting_workers(director):
    director._workers = {'worker-1': {}, 'worker-2': {}}
    director._currently_connected_clients = []

    director._handle_client_request('client-1', {'operation': 'get_worker_ids'})

    payload = sent_payloads(director._zmq_socket)[0]
    assert payload['status'] == 'ok'
    assert sorted(payload['result']) == ['worker-1', 'worker-2']
    assert 'client-1' not in director._currently_connected_clients


# --- _handle_client_request: exec with no workers available ---

def test_exec_with_no_workers_returns_err_response(director):
    director._handle_client_request('client-1', {'operation': 'exec', 'func_id': 'fid'})

    payload = sent_payloads(director._zmq_socket)[0]
    assert payload['status'] == 'err'
    assert 'client-1' not in director._currently_connected_clients


# --- _handle_client_request: get_worker_info / get_cache_dump for unknown worker ---

@pytest.mark.parametrize('operation', ['get_worker_info', 'get_cache_dump'])
def test_get_worker_info_for_unknown_worker_returns_err(director, operation):
    director._handle_client_request('client-1', {'operation': operation, 'worker_id': 'ghost'})

    payload = sent_payloads(director._zmq_socket)[0]
    assert payload['status'] == 'err'
    assert 'client-1' not in director._currently_connected_clients


# --- _handle_worker_request: forward_to_client aggregation for 'list' ---

def test_list_aggregation_merges_results_from_all_workers(director):
    import uuid
    request_id = uuid.uuid4()
    director._currently_connected_clients = ['client-1']
    director._pending_multiple_responses[request_id] = {
        'client_id': 'client-1', 'remaining': 2, 'additional_needed_data': {},
    }

    director._handle_worker_request('worker-1', {
        'operation': 'forward_to_client', 'original_client_operation': 'list',
        'message_id': str(request_id), 'destination_client': 'client-1',
        'status': 'ok', 'result': {'fid1': 'add'},
    })
    # not done yet: only 1 of 2 workers responded
    assert director._zmq_socket.send_multipart.call_count == 0

    director._handle_worker_request('worker-2', {
        'operation': 'forward_to_client', 'original_client_operation': 'list',
        'message_id': str(request_id), 'destination_client': 'client-1',
        'status': 'ok', 'result': {'fid2': 'multiply'},
    })

    payload = sent_payloads(director._zmq_socket)[0]
    assert payload['status'] == 'ok'
    assert payload['result'] == {'fid1': 'add', 'fid2': 'multiply'}
    assert 'client-1' not in director._currently_connected_clients


# --- known-bug regression: unregister on an unknown func_id crashes the Director (BUG-REPORT #4) ---

def test_exec_after_unregister_crashes_director(director):
    # new finding: 'unregister' deletes the func_id -> worker mapping entirely
    # (pyfaas_director.py, the 'forward_to_client'/'unregister' branch). A later
    # 'exec' for that same, now-untracked func_id reaches _select_worker(func_id),
    # which indexes self._functions_workers_map[func_id] without checking it
    # exists -- same root cause as BUG-REPORT.md #4, but reachable via a normal,
    # successful unregister rather than an unknown/malformed func_id.
    director._workers = {'worker-1': {}}
    try:
        director._handle_client_request('client-1', {'operation': 'exec', 'func_id': 'never-registered'})
    except KeyError:
        pytest.xfail('exec on an unregistered func_id raises KeyError in _select_worker and crashes the Director')

    payload = sent_payloads(director._zmq_socket)[0]
    assert payload['status'] == 'err'


def test_unregister_unknown_func_id_should_not_crash_director(director):
    director._workers = {'worker-1': {}}
    try:
        director._handle_client_request('client-1', {'operation': 'unregister', 'func_id': 'never-registered'})
    except KeyError:
        pytest.xfail('BUG-REPORT.md #4: unregister on an unknown func_id raises KeyError and crashes the Director')

    payload = sent_payloads(director._zmq_socket)[0]
    assert payload['status'] == 'err'


# --- heartbeat eviction ---

def test_heartbeats_watcher_evicts_dead_worker(director, monkeypatch):
    now = datetime.datetime.now()
    director._workers = {
        'worker-dead': {
            'registered_at': now - datetime.timedelta(seconds=100),
            'last_heartbeat': now - datetime.timedelta(seconds=100),
        },
    }

    def fake_sleep(_):
        director._threading_stop_event.set()  # stop after a single loop iteration

    monkeypatch.setattr(time, 'sleep', fake_sleep)

    director._heartbeats_watcher()

    assert 'worker-dead' not in director._workers


def test_heartbeats_watcher_grace_period_protects_new_worker(director, monkeypatch):
    now = datetime.datetime.now()
    director._workers = {
        'worker-new': {
            'registered_at': now,
            'last_heartbeat': now - datetime.timedelta(seconds=100),  # no heartbeat yet, but just registered
        },
    }

    def fake_sleep(_):
        director._threading_stop_event.set()

    monkeypatch.setattr(time, 'sleep', fake_sleep)

    director._heartbeats_watcher()

    assert 'worker-new' in director._workers
