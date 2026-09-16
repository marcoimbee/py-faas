import pytest

from pyfaas_worker.app.worker_caching.func_cache import WorkerFunctionExecutionCache


def test_disabled_cache_is_a_no_op():
    cache = WorkerFunctionExecutionCache('LRU', max_size=0)
    cache.add('f1', [1], {}, 'result')
    assert cache.check_cached('f1', [1], {}) is False
    assert cache.get_cache_dump() == {'cache_policy': 'LRU', 'max_size': 0, 'cache': {}}


def test_add_and_retrieve_round_trip():
    cache = WorkerFunctionExecutionCache('LRU', max_size=2)
    cache.add('f1', [1, 2], {'c': 3}, 'result-1')
    assert cache.check_cached('f1', [1, 2], {'c': 3}) is True
    assert cache.get_cached_result('f1', [1, 2], {'c': 3}) == 'result-1'


def test_different_args_are_different_keys():
    cache = WorkerFunctionExecutionCache('LRU', max_size=5)
    cache.add('f1', [1], {}, 'a')
    cache.add('f1', [2], {}, 'b')
    assert cache.get_cached_result('f1', [1], {}) == 'a'
    assert cache.get_cached_result('f1', [2], {}) == 'b'


def test_duplicate_add_raises():
    cache = WorkerFunctionExecutionCache('LRU', max_size=5)
    cache.add('f1', [1], {}, 'a')
    with pytest.raises(Exception):
        cache.add('f1', [1], {}, 'a-again')


def test_get_uncached_result_raises():
    cache = WorkerFunctionExecutionCache('LRU', max_size=5)
    with pytest.raises(Exception):
        cache.get_cached_result('ghost', [], {})


def test_lru_eviction_drops_least_recently_used():
    cache = WorkerFunctionExecutionCache('LRU', max_size=2)
    cache.add('A', [], {}, 'a')
    cache.add('B', [], {}, 'b')
    cache.add('C', [], {}, 'c')  # cache full at insertion time: A is least-recently-used, gets evicted

    assert cache.check_cached('A', [], {}) is False
    assert cache.check_cached('B', [], {}) is True
    assert cache.check_cached('C', [], {}) is True


def test_lru_access_refreshes_recency():
    cache = WorkerFunctionExecutionCache('LRU', max_size=2)
    cache.add('A', [], {}, 'a')
    cache.add('B', [], {}, 'b')
    cache.get_cached_result('A', [], {})  # A is now most-recently-used, B becomes LRU
    cache.add('C', [], {}, 'c')

    assert cache.check_cached('B', [], {}) is False
    assert cache.check_cached('A', [], {}) is True
    assert cache.check_cached('C', [], {}) is True


def test_reset_cache_clears_everything():
    cache = WorkerFunctionExecutionCache('LRU', max_size=5)
    cache.add('A', [], {}, 'a')
    cache.reset_cache()
    assert cache.check_cached('A', [], {}) is False
    assert cache.get_cache_dump()['cache'] == {}


def test_get_cache_dump_reports_entries():
    cache = WorkerFunctionExecutionCache('LRU', max_size=5)
    cache.add('A', [1], {'c': 2}, 'result')
    dump = cache.get_cache_dump()
    assert dump['cache_policy'] == 'LRU'
    assert dump['max_size'] == 5
    (entry,) = dump['cache'].values()
    assert entry == {
        'func_id': 'A',
        'func_positional_args': [1],
        'func_default_args': {'c': 2},
        'func_result': 'result',
    }
