import tomli
import logging
import socket

from exceptions import DirectorConfigError


def read_config_toml(path: str) -> dict:
    with open(path, mode='rb') as fp:
        config = tomli.load(fp)
    
    # Checking director IP addr validity
    try:
        socket.inet_aton(config['network']['director_ip_addr'])
    except socket.error:
        raise DirectorConfigError(f"Config error: invalid or missing field value for 'director_ip_addr': {config['network']['director_ip_addr']} is not a valid IP address")

    # Checking director port validity
    if config['network']['director_port'] is None or config['network']['director_port'] <= 1024 or config['network']['director_port'] >= 65535:
        raise DirectorConfigError(f"Config error: invalid or missing field value for 'director_port': {config['network']['director_port']}")
    
    # Checking heartbeat interval fields
    if config['workers']['heartbeat_check_interval_ms'] is None or config['workers']['heartbeat_check_interval_ms'] <= 0:
        raise DirectorConfigError(f"Config error: invalid or missing field value for 'heartbeat_check_interval_ms': {config['workers']['heartbeat_check_interval_ms']}")
    if config['workers']['expected_heartbeat_interval_ms'] is None or config['workers']['expected_heartbeat_interval_ms'] <= 0:
        raise DirectorConfigError(f"Config error: invalid or missing field value for 'expected_heartbeat_interval_ms': {config['workers']['expected_heartbeat_interval_ms']}") 

    # Checking worker selection strategy
    allowed = ['Round-Robin', 'Random']
    if config['workers']['worker_selection_strategy'] is None or config['workers']['worker_selection_strategy'] not in allowed:
        raise DirectorConfigError(f"Config error: invalid or missing field value for 'worker_selection_strategy': {config['workers']['worker_selection_strategy']}") 

    return config

def setup_logging(log_level: str) -> None:
    match log_level:
        case 'info':
            log_level = logging.INFO
        case 'debug':
            log_level = logging.DEBUG
        case 'warning':
            log_level = logging.WARNING
        case 'critical':
            log_level = logging.CRITICAL
        case 'fatal':
            log_level = logging.FATAL
        case 'error':
            log_level = logging.ERROR
        case _:
            log_level = logging.INFO
    logging.basicConfig(format='[DIRECTOR, %(levelname)s]\t %(message)s', level=log_level, force=True)
