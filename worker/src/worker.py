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
        self.functions = {}
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
                            case "register":
                                serialized_func_base64 = json_payload["serialized_func_base64"]
                                serialized_func_bytes = base64.b64decode(serialized_func_base64)
                                client_function = dill.loads(serialized_func_bytes)
                                func_name = client_function.__name__

                                client_json_response = None
                                if func_name not in self.functions:
                                    self.functions[func_name] = client_function
                                    logging.info(f"Function {func_name} successfully registered")
                                    client_json_response = build_JSON_response(
                                        status="ok", 
                                        action="registered", 
                                        result_type=None, 
                                        result=None,
                                        message=None
                                    )
                                else:
                                    override = json_payload["override"]
                                    if override:
                                        logging.warning(f"A function named '{func_name}' is already registered")
                                        logging.warning("Overriding...")
                                        self.functions[func_name] = client_function
                                        client_json_response = build_JSON_response(
                                            status="ok", 
                                            action="overridden", 
                                            result_type=None, 
                                            result=None, 
                                            message=None
                                        )
                                    else:
                                        logging.warning(f"A function named '{func_name}' is already registered")
                                        logging.warning(f"Function '{func_name}' will not be overridden")
                                        client_json_response = build_JSON_response(
                                            status="ok", 
                                            action="no_action", 
                                            result_type=None, 
                                            result=None, 
                                            message=None
                                        )
                                
                                logging.debug("Currently registered functions:")
                                logging.debug(f"\t {self.functions}")

                                # JSON payload to client
                                conn.sendall(client_json_response)

                            case "unregister":
                                func_name = json_payload["func_name"]

                                client_json_response = None
                                if func_name in self.functions:
                                    logging.info(f"Unregistering '{func_name}'...")
                                    del self.functions[func_name]
                                    if self.config['statistics']['enabled']:
                                        del self.stats[func_name]
                                    client_json_response = build_JSON_response(
                                        status="ok", 
                                        action="unregistered", 
                                        result_type=None, 
                                        result=None, 
                                        message=None
                                    )
                                else:
                                    logging.info(f"No function named '{func_name}' is registered right now")
                                    client_json_response = build_JSON_response(
                                        status="err", 
                                        action="no_func", 
                                        result_type=None, 
                                        result=None, 
                                        message=f"No function named '{func_name}' is registered at the worker right now"
                                    )

                                logging.debug("Currently registered functions:")
                                logging.debug(f"\t {self.functions}")

                                # JSON payload to client
                                conn.sendall(client_json_response)

                            case "exec":
                                func_name = json_payload["func_name"]
                                func_args = json_payload.get("args", [])
                                func_kwargs = json_payload.get("kwargs", {})

                                if func_name not in self.functions:
                                    logging.info(f"No function named '{func_name}' is registered right now")
                                    client_json_response = build_JSON_response(
                                        status="err", 
                                        action="no_func", 
                                        result_type=None, 
                                        result=None, 
                                        message=f"No function named '{func_name}' is registered at the worker right now"
                                    )
                                    conn.sendall(client_json_response)
                                else:
                                    try:
                                        logging.info(f"Executing the following call: {func_name}({func_args}, {func_kwargs})")

                                        client_function = self.functions[func_name]

                                        start_time = time.time()
                                        func_res = client_function(*func_args, **func_kwargs)
                                        end_time = time.time()

                                        exec_time = end_time - start_time
                                        self.record_stats(func_name, exec_time)

                                        logging.info(f"Executed '{func_name}' for {client_addr[0]}:{client_addr[1]} in {exec_time} s")
                                        logging.debug(f"{func_name} data: \n \t{self.stats[func_name]}")
                                        logging.debug(f"Function result: {func_res}")

                                        encoded_func_res, func_res_type = encode_func_result(func_res)          # JSON or base64
                                        client_json_response = build_JSON_response(
                                            status="ok", 
                                            action="executed", 
                                            result_type=func_res_type, 
                                            result=encoded_func_res,
                                            message=None
                                        )

                                        # send back result
                                        conn.sendall(client_json_response)
                                        
                                    except Exception as e:
                                        client_json_response = build_JSON_response(
                                            status="err", 
                                            action=None, 
                                            result_type="json", 
                                            result=None,
                                            message=f"{type(e).__name__}: {e}"
                                        )
                                        conn.sendall(client_json_response)

                            case "list":
                                try:
                                    func_list = [f for f, _ in self.functions.items()]
                                    logging.info(f"List: retrieved {len(func_list)} functions")

                                    client_json_response = build_JSON_response(
                                        status="ok", 
                                        action=None, 
                                        result_type="json", 
                                        result=func_list,
                                        message=None
                                    )

                                    # send back result
                                    conn.sendall(client_json_response)
                                    
                                except Exception as e:
                                    client_json_response = build_JSON_response(
                                        status="err", 
                                        action=None, 
                                        result_type="json", 
                                        result=None,
                                        message=f"{type(e).__name__}: {e}"
                                    )
                                    conn.sendall(client_json_response)

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
            

def build_JSON_response(status, action, result_type, result, message):
    return json.dumps({
        "status": status,
        "action": action,
        "result_type": result_type,
        "result": result,
        "message": message
    }).encode()

def encode_func_result(func_result):
    try:
        json.dumps(func_result)      # Test JSON-serializability, return plain result if successful
        return func_result, "json"
    except (TypeError, OverflowError):      # result is not JSON-serializable, let caller know
        func_result_bytes = dill.dumps(func_result)
        func_result_base64 = base64.b64encode(func_result_bytes).decode()
        return func_result_base64, "pickle_base64"



def main():    
    config = util.read_config_toml(TOML_CONFIG_FILE)

    util.setup_logging(config['misc']['log_level'])

    worker = PyfaasWorker(config)
    worker.run()


if __name__ == "__main__":
    main()
