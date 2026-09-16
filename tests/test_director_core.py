import datetime
import json
import time
from unittest.mock import MagicMock

import pytest

from pyfaas_director.app.exceptions import DirectorConfigError, DirectorNoAvailableWorkersError
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


# --- known-bug regression: an unrecognized func_id crashes the Director instead of erroring gracefully ---

def test_exec_after_unregister_crashes_director(director):
    # 'unregister' deletes the func_id -> worker mapping entirely (the
    # 'forward_to_client'/'unregister' branch of _handle_worker_request). A later
    # 'exec' for that same, now-untracked func_id reaches _select_worker(func_id),
    # which indexes self._functions_workers_map[func_id] without checking it
    # exists -- reachable via a normal, successful unregister followed by exec,
    # not just a malformed/unknown func_id.
    director._workers = {'worker-1': {}}
    try:
        director._handle_client_request('client-1', {'operation': 'exec', 'func_id': 'never-registered'})
    except KeyError:
        pytest.xfail('exec on an unregistered func_id raises KeyError in _select_worker and crashes the Director')

    payload = sent_payloads(director._zmq_socket)[0]
    assert payload['status'] == 'err'


# --- unregister on an unknown func_id: fixed ---

def test_unregister_unknown_func_id_returns_err(director):
    director._workers = {'worker-1': {}}
    director._handle_client_request('client-1', {'operation': 'unregister', 'func_id': 'never-registered'})

    payload = sent_payloads(director._zmq_socket)[0]
    assert payload['status'] == 'err'
    assert 'client-1' not in director._currently_connected_clients


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


# --- known-bug regression: chain_exec is silently dropped across unsynchronized multi-worker clusters ---

def test_chain_exec_on_unsynchronized_multi_worker_cluster_should_not_be_silently_dropped(director):
    # _handle_client_request's 'chain_exec' case does nothing (`pass`) in the
    # unsynchronized, >1-worker branch: no response is ever sent, and the client
    # is never removed from _currently_connected_clients, which also permanently
    # blocks _synchronize_workers (it refuses to run while any client is pending).
    director._workers = {'worker-1': {}, 'worker-2': {}}
    director._workers_are_synchronized = False
    func_id = director._compute_function_id('add', 'code')
    director._functions_workers_map[func_id] = {
        'func_name': 'add', 'registering_client': 'client-1', 'available_on': ['worker-1'],
    }
    workflow = {
        'id': 'wf1', 'entry_function': 'add',
        'functions': {'add': {'positional_args': [], 'default_args': {}, 'next': '', 'cache_result': False}},
    }

    director._handle_client_request('client-1', {'operation': 'chain_exec', 'json_workflow': workflow})

    if director._zmq_socket.send_multipart.call_count == 0 and 'client-1' in director._currently_connected_clients:
        pytest.xfail('chain_exec on an unsynchronized multi-worker cluster sends no response and never '
                      'releases the client, permanently blocking worker synchronization')


# --- known-bug regression: chain_exec has no server-side structural validation ---

def test_chain_exec_malformed_workflow_should_not_crash_director(director):
    # _handle_client_request's 'chain_exec' case does
    # workflow_function_set.items() where workflow_function_set = json_workflow.get('functions'),
    # trusting the client to have already run client-side validation. A client
    # that skips it (or talks to the ZMQ socket directly) can send a workflow
    # with no 'functions' key at all and crash the Director.
    director._workers = {'worker-1': {}}
    try:
        director._handle_client_request('client-1', {
            'operation': 'chain_exec', 'json_workflow': {'id': 'wf1', 'entry_function': 'add'},
        })
    except AttributeError:
        pytest.xfail('chain_exec with a malformed workflow (missing "functions") raises an uncaught '
                      'AttributeError and crashes the Director')

    payload = sent_payloads(director._zmq_socket)[0]
    assert payload['status'] == 'err'


# --- known-bug regression: 'unregister' aggregation uses only the last Worker response ---

def test_unregister_aggregation_should_consider_all_worker_responses(director):
    # Unlike 'list'/'get_stats' (which use any(status == 'err' ...) across every
    # response), the 'unregister' branch only inspects the LAST response that
    # arrives. If workers disagree, the outcome depends on network arrival order
    # instead of being deterministic -- here the first (discarded) response is an
    # error but the last (decisive) one is 'ok', so the func_id gets removed even
    # though one worker reported a problem.
    import uuid
    request_id = uuid.uuid4()
    func_id = 'fid1'
    director._currently_connected_clients = ['client-1']
    director._functions_workers_map[func_id] = {'func_name': 'add', 'registering_client': 'client-1', 'available_on': ['worker-1', 'worker-2']}
    director._pending_multiple_responses[request_id] = {
        'client_id': 'client-1', 'remaining': 2, 'additional_needed_data': {'func_id': func_id},
    }

    director._handle_worker_request('worker-1', {
        'operation': 'forward_to_client', 'original_client_operation': 'unregister',
        'message_id': str(request_id), 'destination_client': 'client-1', 'status': 'err',
    })
    director._handle_worker_request('worker-2', {
        'operation': 'forward_to_client', 'original_client_operation': 'unregister',
        'message_id': str(request_id), 'destination_client': 'client-1', 'status': 'ok',
    })

    if func_id not in director._functions_workers_map:
        pytest.xfail("unregister aggregation only inspects the last worker's response (here 'ok'), "
                      "ignoring an earlier 'err' response from a different worker")


# --- known-bug regression: dead-worker cleanup leaves stale _functions_workers_map entries ---

def test_dead_worker_removal_should_clean_up_functions_workers_map(director, monkeypatch):
    # _heartbeats_watcher deletes the dead worker from self._workers but never
    # touches _functions_workers_map, so a function whose only home was that
    # worker is still listed as available there -- future exec/unregister
    # requests for it get routed to a ZMQ identity that no longer exists.
    now = datetime.datetime.now()
    director._workers = {
        'worker-dead': {'registered_at': now - datetime.timedelta(seconds=100), 'last_heartbeat': now - datetime.timedelta(seconds=100)},
    }
    director._functions_workers_map['fid1'] = {'func_name': 'add', 'registering_client': 'client-1', 'available_on': ['worker-dead']}

    def fake_sleep(_):
        director._threading_stop_event.set()

    monkeypatch.setattr(time, 'sleep', fake_sleep)
    director._heartbeats_watcher()

    if director._functions_workers_map['fid1']['available_on'] == ['worker-dead']:
        pytest.xfail('dead-worker eviction does not purge/rebuild stale entries in _functions_workers_map')


# --- _select_worker's func_id-specific match still lacks its own case _: (cosmetic only) ---

def test_select_worker_for_func_with_unknown_strategy_raises_via_fallthrough(director):
    # The func_id-specific match (used when a function is available on more than
    # one, not-yet-synchronized worker) still has no `case _:` of its own. But an
    # unmatched `match` is a no-op, not a `return` -- execution falls out of the
    # `if func_id is not None:` block entirely and into the general match right
    # below it, which DOES have a `case _:` and raises. Since both matches read
    # the same self._worker_selection_strategy, this ends up safe in practice:
    # confirmed here rather than assumed.
    director._workers = {'worker-1': {}, 'worker-2': {}}
    director._functions_workers_map['fid1'] = {'available_on': ['worker-1', 'worker-2']}
    director._worker_selection_strategy = 'Fastest'  # not a real strategy

    with pytest.raises(DirectorConfigError):
        director._select_worker('fid1')
