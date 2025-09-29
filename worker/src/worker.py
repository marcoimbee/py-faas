import util
import socket
import logging
import dill
import time
import json
import base64
import inspect
import datetime


TOML_CONFIG_FILE = "worker/worker_config.toml"
MAX_DATA = 65536

class PyfaasWorker:
    def __init__(self, config):
        self.host = config['network']['worker_ip_addr']
        self.port = config['network']['worker_port']
        self.config = config
        self.stats = {}

        logging.info(config['misc']['greeting_msg'])


    def run(self):
        worker_ip_port_tuple = (self.host, self.port)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(worker_ip_port_tuple)
            s.listen()
            s.settimeout(1.0)     # To be able to catch ctrl+c
            logging.info(f"Worker reachable at: {worker_ip_port_tuple[0]}:{worker_ip_port_tuple[1]}")
            
            try:
                while True:
                    try:
                        conn, client_addr = s.accept()
                    except socket.timeout:
                        continue

                    with conn:
                        logging.debug(f"Worker connected by {client_addr}")
                        recvd_data = conn.recv(MAX_DATA).decode()

                        json_payload = json.loads(recvd_data)
                        
                        # split command + command args
                        cmd = json_payload["cmd"]

                        match cmd:
                            case "exec":
                                serialized_func_base64 = json_payload["serialized_func_base64"]
                                serialized_func_bytes = base64.b64decode(serialized_func_base64)

                                func_args = json_payload["args"]
                                func_kwargs = json_payload["kwargs"]

                                client_function = dill.loads(serialized_func_bytes)

                                logging.info(f"Executing the following call: {client_function.__name__}({func_args}, {func_kwargs})...")
                                start_time = time.time()
                                res = client_function(*func_args, **func_kwargs)
                                end_time = time.time()
                                
                                exec_time = end_time - start_time
                                self.record_stats(client_function.__name__, exec_time)
                                
                                logging.info(f"Executed {client_function.__name__} for {client_addr[0]}:{client_addr[1]} in {exec_time} s")
                                logging.debug(f"{client_function.__name__} data: \n \t{self.stats[client_function.__name__]}")

                                # send back result
                                res_bytes = dill.dumps(res)
                                conn.sendall(res_bytes)

                            case "get_stats":
                                logging.info("get_stats() still not implemented")

                            case "kill":
                                logging.info(f"Worker killed by client at {datetime.datetime.now()}")
                                return

                            case "PING":
                                logging.info(f"Client says: '{cmd}'")
                                ping_resp = "PONG"
                                conn.sendall(ping_resp.encode())

                            case _:
                                logging.warning(f"Client specified unknown command '{cmd}'")

            except KeyboardInterrupt:
                logging.info("Goodbye")

    def record_stats(self, func_name, exec_time) -> None:
        if self.config['statistics']['enabled']:
            if func_name not in self.stats:
                self.stats[func_name] = {}
                self.stats[func_name]["#calls"] = 1
                self.stats[func_name]["avg_exec_time"] = exec_time
                self.stats[func_name]["tot_exec_time"] = exec_time
            else:
                self.stats[func_name]["#calls"] += 1
                self.stats[func_name]["tot_exec_time"] += exec_time
                avg_exec_time = self.stats[func_name]["tot_exec_time"] / self.stats[func_name]["#calls"]
                self.stats[func_name]["avg_exec_time"] = avg_exec_time
        else:
            logging.info("Statistics have not been enabled")    
            


def main():    
    config = util.read_config_toml(TOML_CONFIG_FILE)

    util.setup_logging(config['misc']['log_level'])

    worker = PyfaasWorker(config)
    worker.run()


if __name__ == "__main__":
    main()
