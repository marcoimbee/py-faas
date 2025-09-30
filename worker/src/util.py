import tomli
import logging
import socket


# TODO: define custom exception classes
def read_config_toml(path: str) -> dict:
    with open(path, mode="rb") as fp:
        config = tomli.load(fp)
    
    # Checking port validity
    if config['network']['worker_port'] == None or config['network']['worker_port'] <= 1024 or config['network']['worker_port'] >= 65535:
        raise Exception(f"Config error: invalid port {config['network']['worker_port']}")
    
    # Checking IP addr validity
    try:
        socket.inet_aton(config['network']['worker_ip_addr'])
    except socket.error:
        raise Exception(f"Config error: invalid IP Address {config['network']['worker_ip_addr']}")

    return config

    
def setup_logging(log_level: str) -> None:
    match log_level:
        case "info":
            log_level = logging.INFO
        case "debug":
            log_level = logging.DEBUG
        case "warning":
            log_level = logging.WARNING
        case "critical":
            log_level = logging.CRITICAL
        case "fatal":
            log_level = logging.FATAL
        case "error":
            log_level = logging.ERROR
        case _:
            log_level = logging.INFO
    logging.basicConfig(format='[WORKER, %(levelname)s]\t %(message)s', level=log_level, force=True)
