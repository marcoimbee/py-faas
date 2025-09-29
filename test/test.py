import time

from pyfaas import (
    pyfaas_exec, 
    pyfaas_ping, 
    pyfaas_kill_worker, 
    pyfaas_config
)


def simple_fun(a: int, b: int, c: int = 18) -> int:
    time.sleep(1)
    return a + b * c


if __name__ == "__main__":
    pyfaas_config("test/client_config.toml")

    args = (12, 69)
    kwargs = {"c": 21}
    res = pyfaas_exec(simple_fun, args, kwargs, None)
    print(res)

    pyfaas_ping()

    pyfaas_kill_worker()

