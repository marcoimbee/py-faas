import base64
import json
import queue
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import dill
import pytest

from pyfaas_worker.app.worker_caching.func_cache import WorkerFunctionExecutionCache
from pyfaas_worker.app.worker_operations import WorkerOperations


@pytest.fixture
def worker_stub():
    return SimpleNamespace(
        _id='worker-test',
        _logger=MagicMock(),
        _file_logger=MagicMock(),
        _lock=threading.RLock(),
        _functions={},
        _stats={},
        _outgoing_tx_queue=queue.Queue(),
        _function_exec_cache=WorkerFunctionExecutionCache('LRU', max_size=10),
        _config={'statistics': {'enabled': True}, 'logging': {'log_level': 'debug'}},
        _request_count=0,
        _start_time=None,
        _last_client_connection_ts=None,
    )


@pytest.fixture
def ops(worker_stub):
    return WorkerOperations(worker_stub)


def last_response(worker_stub):
    return json.loads(worker_stub._outgoing_tx_queue.get_nowait()[1].decode())


def add(a: int, b: int, c: int = 10) -> int:
    return a + b + c


def missing_param_annotation(a, b: int) -> int:
    return a + b


def missing_return_annotation(a: int, b: int):
    return a + b


# --- register ---

def test_register_success(ops, worker_stub):
    ops.execute_register_cmd({
        'requester': 'client-1', 'serialized_func_base64': _encode(add), 'func_id': 'fid1',
    })

    response = last_response(worker_stub)
    assert response['status'] == 'ok'
    assert response['action'] == 'registered'
    assert response['result'] == 'fid1'
    assert worker_stub._functions['fid1']['name'] == 'add'
    assert worker_stub._functions['fid1']['registering_client'] == 'client-1'
    assert worker_stub._stats['fid1'] == {}


def test_register_missing_param_annotation_rejected(ops, worker_stub):
    ops.execute_register_cmd({
        'requester': 'client-1', 'serialized_func_base64': _encode(missing_param_annotation), 'func_id': 'fid1',
    })

    response = last_response(worker_stub)
    assert response['status'] == 'err'
    assert 'fid1' not in worker_stub._functions


def test_register_missing_return_annotation_rejected(ops, worker_stub):
    ops.execute_register_cmd({
        'requester': 'client-1', 'serialized_func_base64': _encode(missing_return_annotation), 'func_id': 'fid1',
    })

    response = last_response(worker_stub)
    assert response['status'] == 'err'
    assert 'fid1' not in worker_stub._functions


def test_register_already_registered_is_a_no_op(ops, worker_stub):
    worker_stub._functions['fid1'] = {'name': 'add', 'code': add, 'registering_client': 'client-1'}

    ops.execute_register_cmd({
        'requester': 'client-1', 'serialized_func_base64': _encode(add), 'func_id': 'fid1',
    })

    response = last_response(worker_stub)
    assert response['status'] == 'ok'
    assert response['action'] == 'no_action'


def _encode(func):
    return base64.b64encode(dill.dumps(func)).decode('utf-8')


# --- ping ---

def test_ping_responds_pong(ops, worker_stub):
    ops.execute_ping_cmd({'requester': 'client-1'})
    response = last_response(worker_stub)
    assert response['status'] == 'ok'
    assert response['result'] == 'PONG'


# --- list ---

def test_list_filters_by_requesting_client(ops, worker_stub):
    worker_stub._functions = {
        'fid1': {'name': 'add', 'registering_client': 'client-1'},
        'fid2': {'name': 'multiply', 'registering_client': 'client-2'},
    }

    ops.execute_list_cmd({'requester': 'client-1', 'request_id': 'req-1'})

    response = last_response(worker_stub)
    assert response['status'] == 'ok'
    assert response['result'] == {'fid1': 'add'}


# --- unregister ---

def test_unregister_success_removes_function_and_stats(ops, worker_stub):
    worker_stub._functions['fid1'] = {'name': 'add', 'code': add, 'registering_client': 'client-1'}
    worker_stub._stats['fid1'] = {}

    ops.execute_unregister_cmd({'requester': 'client-1', 'request_id': 'req-1', 'func_id': 'fid1'})

    response = last_response(worker_stub)
    assert response['status'] == 'ok'
    assert response['action'] == 'unregistered'
    assert 'fid1' not in worker_stub._functions
    assert 'fid1' not in worker_stub._stats


def test_unregister_by_non_owner_is_forbidden(ops, worker_stub):
    worker_stub._functions['fid1'] = {'name': 'add', 'code': add, 'registering_client': 'client-1'}

    ops.execute_unregister_cmd({'requester': 'client-2', 'request_id': 'req-1', 'func_id': 'fid1'})

    response = last_response(worker_stub)
    assert response['status'] == 'err'
    assert response['action'] == 'forbidden'
    assert 'fid1' in worker_stub._functions


def test_unregister_unknown_func_id(ops, worker_stub):
    ops.execute_unregister_cmd({'requester': 'client-1', 'request_id': 'req-1', 'func_id': 'ghost'})

    response = last_response(worker_stub)
    assert response['status'] == 'err'
    assert response['action'] == 'no_func'


# --- get_stats ---

def test_get_stats_filters_by_requesting_client(ops, worker_stub):
    worker_stub._functions = {
        'fid1': {'name': 'add', 'registering_client': 'client-1'},
        'fid2': {'name': 'multiply', 'registering_client': 'client-2'},
    }
    worker_stub._stats = {'fid1': {'#calls': 3}, 'fid2': {'#calls': 5}}

    ops.execute_get_stats_cmd({'requester': 'client-1', 'request_id': 'req-1'})

    response = last_response(worker_stub)
    assert response['result'] == {'fid1': {'#calls': 3}}


# --- exec ---

def test_exec_unknown_func_id_returns_err(ops, worker_stub):
    ops.execute_exec_cmd({'requester': 'client-1', 'func_id': 'ghost', 'positional_args': [], 'default_args': {}})

    response = last_response(worker_stub)
    assert response['status'] == 'err'
    assert response['action'] == 'no_func'


def test_exec_success_returns_json_result_and_records_stats(ops, worker_stub):
    worker_stub._functions['fid1'] = {'name': 'add', 'code': add, 'registering_client': 'client-1'}
    worker_stub._stats['fid1'] = {}

    ops.execute_exec_cmd({
        'requester': 'client-1', 'func_id': 'fid1', 'positional_args': [1, 2], 'default_args': {'c': 3},
    })

    response = last_response(worker_stub)
    assert response['status'] == 'ok'
    assert response['action'] == 'executed'
    assert response['result_type'] == 'json'
    assert response['result'] == 6
    assert worker_stub._stats['fid1']['#calls'] == 1


def test_exec_non_json_result_is_pickled(ops, worker_stub):
    def returns_a_set() -> set:
        return {1, 2, 3}

    worker_stub._functions['fid1'] = {'name': 'returns_a_set', 'code': returns_a_set, 'registering_client': 'client-1'}
    worker_stub._stats['fid1'] = {}

    ops.execute_exec_cmd({'requester': 'client-1', 'func_id': 'fid1', 'positional_args': [], 'default_args': {}})

    response = last_response(worker_stub)
    assert response['result_type'] == 'pickle_base64'
    assert dill.loads(base64.b64decode(response['result'])) == {1, 2, 3}


def test_exec_raising_function_returns_err(ops, worker_stub):
    def boom() -> int:
        raise ValueError('kaboom')

    worker_stub._functions['fid1'] = {'name': 'boom', 'code': boom, 'registering_client': 'client-1'}
    worker_stub._stats['fid1'] = {}

    ops.execute_exec_cmd({'requester': 'client-1', 'func_id': 'fid1', 'positional_args': [], 'default_args': {}})

    response = last_response(worker_stub)
    assert response['status'] == 'err'
    assert 'kaboom' in response['message']


@pytest.mark.xfail(reason="new finding: execute_exec_cmd does `.get('default_args', {})`, but the low-level "
                           'PyfaasClient.pyfaas_exec() sends an explicit default_args=None (its own default '
                           "parameter value) instead of omitting the key -- .get() then returns None rather than "
                           '{}, and **None crashes cache-key building before the function ever runs. Only the '
                           'higher-level pyfaas.pyfaas_exec() wrapper works around this by normalizing None to {} '
                           'itself before calling the client.')
def test_exec_none_default_args_crashes(ops, worker_stub):
    worker_stub._functions['fid1'] = {'name': 'add', 'code': add, 'registering_client': 'client-1'}
    worker_stub._stats['fid1'] = {}

    ops.execute_exec_cmd({
        'requester': 'client-1', 'func_id': 'fid1', 'positional_args': [1, 2], 'default_args': None,
    })

    response = last_response(worker_stub)
    assert response['status'] == 'ok'
    assert response['result'] == 13


def test_exec_saves_result_in_cache_when_requested(ops, worker_stub):
    worker_stub._functions['fid1'] = {'name': 'add', 'code': add, 'registering_client': 'client-1'}
    worker_stub._stats['fid1'] = {}

    ops.execute_exec_cmd({
        'requester': 'client-1', 'func_id': 'fid1', 'positional_args': [1, 2],
        'default_args': {'c': 3}, 'save_in_cache': True,
    })

    assert worker_stub._function_exec_cache.check_cached('fid1', [1, 2], {'c': 3}) is True


def test_exec_cache_hit_returns_cached_result(ops, worker_stub):
    worker_stub._functions['fid1'] = {'name': 'add', 'code': add, 'registering_client': 'client-1'}
    worker_stub._stats['fid1'] = {}
    worker_stub._function_exec_cache.add('fid1', [1, 2], {'c': 3}, 6)

    ops.execute_exec_cmd({
        'requester': 'client-1', 'func_id': 'fid1', 'positional_args': [1, 2], 'default_args': {'c': 3},
    })

    response = last_response(worker_stub)
    assert response['status'] == 'ok'
    assert response['result'] == 6


# --- chain_exec (known to be entirely broken, see BUG-REPORT.md #1) ---

@pytest.mark.xfail(reason='BUG-REPORT.md #1: chain_exec confuses function names with function IDs and calls a '
                           'nonexistent _check_function_set_registration(), so every chain_exec request raises '
                           'AttributeError instead of executing the workflow')
def test_chain_exec_runs_a_two_function_workflow(ops, worker_stub):
    worker_stub._functions = {
        'fid_add': {'name': 'add', 'code': add, 'registering_client': 'client-1'},
        'fid_mul': {'name': 'multiply', 'code': lambda a, b: a * b, 'registering_client': 'client-1'},
    }
    workflow = {
        'id': 'wf1',
        'entry_function': 'add',
        'functions': {
            'add': {'positional_args': [2, 3], 'default_args': {}, 'next': 'multiply', 'cache_result': False},
            'multiply': {'positional_args': ['$add.output', 10], 'default_args': {}, 'next': '', 'cache_result': False},
        },
    }

    ops.execute_chain_exec_cmd({'requester': 'client-1', 'json_workflow': workflow})

    response = last_response(worker_stub)
    assert response['status'] == 'ok'
    assert response['result'] == 150


# --- pure helpers ---

def test_encode_func_result_keeps_json_serializable_values(ops):
    encoded, result_type = ops._encode_func_result({'a': 1})
    assert result_type == 'json'
    assert encoded == {'a': 1}


def test_encode_func_result_pickles_non_json_values(ops):
    encoded, result_type = ops._encode_func_result({1, 2, 3})
    assert result_type == 'pickle_base64'
    assert dill.loads(base64.b64decode(encoded)) == {1, 2, 3}
