import time

from pyfaas import (
    pyfaas_exec, 
    pyfaas_ping, 
    pyfaas_kill_worker, 
    pyfaas_config,
    pyfaas_register,
    pyfaas_unregister,
    pyfaas_list
)


def simple_fun(a: int, b: int, c: int = 18) -> int:
    time.sleep(1)
    return a + b + c

def simple_fun2(a: int, b: int, c: int = 18) -> int:
    time.sleep(1)
    return a + b + c

def simple_fun3(a: int, b: int, c: int = 18) -> int:
    time.sleep(1)
    return a + b + c

if __name__ == "__main__":
    pyfaas_config("test/client_config.toml")
    
    pyfaas_register(simple_fun, True)

    l = pyfaas_list()
    print(l)

    pyfaas_register(simple_fun2, True)
    print(pyfaas_list())

    pyfaas_register(simple_fun3, True)
    print(pyfaas_list())

    pyfaas_unregister("simple_fun")
    pyfaas_unregister("simple_fun2")
    pyfaas_unregister("simple_fun3")

    l = pyfaas_list()
    print(l)