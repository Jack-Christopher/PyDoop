import socket
from map_reduce import mapper, reducer

class Slave:
    def __init__(self, config):
        self.config = config
        # files that should be processed by this slave
        self.files = []
        self.result = None


        full_data_path = config['data_path']
        if config['dev_mode']:
            self.data_path = full_data_path+"/devdata"
        else:
            self.data_path = full_data_path+"/fulldata"

    def start_client(self):
        host = self.config['name_node']['host']
        port = self.config['name_node']['port']
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((host, port))

        instructions = client_socket.recv(1024)
        self.files = (str(eval(instructions.decode('utf-8')))).replace("[","").replace("]","").replace("'","").replace(" ","").split(",")
        print("[Slave]: files => "+str(eval(instructions.decode('utf-8'))))

        # client_socket.close()

        self.client_mapper(client_socket)



    def client_mapper(self, client_socket):
        self.result = mapper(self.files, self.data_path)
        print("[Slave] ["+str(client_socket.getsockname())+"]: Resultados parciales: "+str(self.result)[0:100])
        
        # host = self.config['name_node']['host']
        # port = self.config['name_node']['port']
        # client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # client_socket.connect((host, port))
        
        # itesrator over the result
        for key in self.result.keys():
            # a dict with a single key-value pair
            client_socket.sendall(str.encode(str({key:self.result[key]})))
            # print  sending
            print("[Slave] ["+str(client_socket.getsockname())+"]: Enviando subresultados a NameNode: "+str({key:self.result[key]}))
        client_socket.close()
        