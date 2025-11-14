import logging
import tomli
import socket


def read_config_toml(path: str) -> dict:
    with open(path, mode='rb') as fp:
        config = tomli.load(fp)
    
    # Checking director port validity
    if config['network']['director_port'] is None or config['network']['director_port'] <= 1024 or config['network']['director_port'] >= 65535:
        raise Exception(f'Config error: invalid director port {config['network']['director_port']}')
    
    # Checking director IP addr validity
    try:
        socket.inet_aton(config['network']['director_ip_addr'])
    except socket.error:
        raise Exception(f'Config error: invalid director IP address {config['network']['director_ip_addr']}')
    
    # Checking receive timeout field
    if type(config['network']['receive_timeout_s']) != int:
        raise Exception(f"Config error: field 'receive_timeout_s' must be of type 'str', while '{type(config['network']['receive_timeout_s'])}' was provided")
    if config['network']['receive_timeout_s'] < 0:
        raise Exception(f"Config error: invalid value {config['network']['receive_timeout_s']} for field 'receive_timeout_s'")

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
        case '':
            log_level = None
        case _:
            log_level = logging.INFO
    logging.basicConfig(format='[PYFAAS, %(levelname)s]\t %(message)s', level=log_level, force=True)
