import json


class WorkerFunctionExecutionCache():
    class CachedResultNode():
        def __init__(self, func_name: str = None, func_positional_args: list[object] = None, func_default_args: dict[object] = None, func_result: object = None):
            self.func_name = func_name
            self.func_positional_args = func_positional_args
            self.func_default_args = func_default_args
            self.func_result = func_result
            self.next = None
            self.prev = None

    def __init__(self, policy: str, max_size: int):
        self._policy = policy
        self._max_size = max_size
        self._cache_nodes_hmap = {}
        self._cache_head = self.CachedResultNode()
        self._cache_tail = self.CachedResultNode()
        self._cache_head.next = self._cache_tail
        self._cache_tail.prev = self._cache_head

    def _build_key_tuple(self, func_name: str, func_positional_args: list[object], func_default_args: dict[object]) -> tuple:
        return (
            func_name,
            tuple(func_positional_args),
            frozenset(func_default_args.items())
        )

    def add(self, func_name: str, func_positional_args: list[object], func_default_args: dict[object], func_result: object) -> None:
        if self._max_size == 0:
            # Caching is disabled
            return

        key_tuple = self._build_key_tuple(func_name, func_positional_args, func_default_args)
        if key_tuple in self._cache_nodes_hmap:
            # No duplicates allowed
            # No nodes update allowed
            # This should never happen. However, we are covered with this
            raise Exception(f"No cache duplicates allowed: '{key_tuple}'")
        else:
            if len(self._cache_nodes_hmap) == self._max_size:
                last_used_result = self._cache_tail.prev
                self._cache_tail.prev = self._cache_tail.prev.prev
                self._cache_tail.prev.next = self._cache_tail
                last_used_result_key_tuple = self._build_key_tuple(
                    last_used_result.func_name, 
                    last_used_result.func_positional_args,
                    last_used_result.func_default_args
                ) 
                del self._cache_nodes_hmap[last_used_result_key_tuple]            
            
            new_cached_result = self.CachedResultNode(func_name, func_positional_args, func_default_args, func_result)
            
            new_cached_result.next = self._cache_head.next
            new_cached_result.prev = self._cache_head
            self._cache_head.next.prev = new_cached_result
            self._cache_head.next = new_cached_result

            self._cache_nodes_hmap[key_tuple] = new_cached_result


    def get_cached_result(self, func_name: str, func_positional_args: list[object], func_default_args: dict[object]) -> object:
        if self._max_size == 0:
            # Caching is disabled
            return None

        key_tuple = self._build_key_tuple(func_name, func_positional_args, func_default_args)
        if not self.check_cached(func_name, func_positional_args, func_default_args):
            raise Exception(f"'{key_tuple}' is currently not in the cache")
        
        cached_result = self._cache_nodes_hmap.get(key_tuple)

        # Unlinking node
        cached_result.prev.next = cached_result.next
        cached_result.next.prev = cached_result.prev

        # Moving node to front of list
        cached_result.next = self._cache_head.next
        self._cache_head.next.prev = cached_result
        cached_result.prev = self._cache_head
        self._cache_head.next = cached_result

        return cached_result.func_result

    def check_cached(self, func_name: str, func_positional_args: list[object], func_default_args: dict[object]) -> bool:
        if self._max_size == 0:
            return False

        key_tuple = self._build_key_tuple(func_name, func_positional_args, func_default_args)
        if key_tuple in self._cache_nodes_hmap:
            return True
        return False
        
    def reset_cache(self):
        self._cache_nodes_hmap = {}
        self._cache_head.next = self._cache_tail
        self._cache_tail.prev = self._cache_head        

    def get_cache_dump(self) -> dict:
        if self._max_size == 0:
            # Caching is disabled
            return {
                'cache_policy': self._policy,
                'max_size': 0,
                'cache': {}
            }

        def serialize_key(func_name, func_positional_args, func_default_args):
            return {
                'func_name': func_name,
                'func_positional_args': list(func_positional_args),         # tuple to list
                'func_default_args': dict(func_default_args)      # frozenset to dict
            }
        
        cache_dump = {}
        for (func_name, func_positional_args, func_default_args), node in self._cache_nodes_hmap.items():
            key_str = json.dumps(serialize_key(func_name, func_positional_args, func_default_args))
            cache_dump[key_str] = {
                'func_name': node.func_name,
                'func_positional_args': node.func_positiobal_args,     # already list
                'func_default_args': node.func_default_args,         # already dict
                'func_result': node.func_result
            }
        
        return {
            'cache_policy': self._policy,
            'max_size': self._max_size,
            'cache': cache_dump
        }
