import socket
from map_reduce import mapper, reducer

class Slave:
    def _init_(self, config):
        self.config = config
        self.files = []
        self.result = None
        self.data_path = self.config['data_path'] + ("/devdata" if self.config['dev_mode'] else "/fulldata")

    def start_client(self):
        host = self.config['name_node']['host']
        port = self.config['name_node']['port']
        
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((host, port))
        except ConnectionRefusedError:
            print(f"Error: Connection refused. Please make sure the NameNode is running at {host}:{port}")
            return

        instructions = client_socket.recv(1024)
        self.files = eval(instructions.decode('utf-8'))
        print("[Slave]: files => " + str(self.files))

        self.client_mapper(client_socket)
        client_socket.close()

    def client_mapper(self, client_socket):
        self.result = mapper(self.files, self.data_path)
        print("[Slave] [" + str(client_socket.getsockname()) + "]: Partial results: " + str(self.result)[:100])

        for key, value in self.result.items():
            subresult = {key: value}
            client_socket.sendall(str(subresult).encode('utf-8'))
            print("[Slave] [" + str(client_socket.getsockname()) + "]: Sending subresults to NameNode: " + str(subresult))