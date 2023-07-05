import os
import socket
from map_reduce import reducer

class Master:
    def __init__(self, config):
        self.config = config
        self.num_data_nodes = config['num_data_nodes']
        self.main_result = None
        self.server_socket = None

        full_data_path = config['data_path']
        if config['dev_mode']:
            self.data_path = full_data_path+"/devdata"
        else:
            self.data_path = full_data_path+"/fulldata"

        self.partitions = [ [] for _ in range(self.num_data_nodes) ]
        # iterate over the files inside data_path and append their names to the partitions list
        temp = 0
        for f in os.listdir(self.data_path):
            if os.path.isfile(os.path.join(self.data_path, f)):
                self.partitions[temp].append(f)
                temp = (temp + 1) % self.num_data_nodes


    def start_server(self):
        host = self.config['name_node']['host']
        port = self.config['name_node']['port']
        print('[NameNode]: Iniciando servidor en '+str(host)+':'+str(port))
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((host, port))
        self.server_socket.listen(1)
        print('[NameNode]: Esperando conexiones...')

        # dict of connected data nodes
        connected_data_nodes = set()

        while len(connected_data_nodes) < self.num_data_nodes:
            client_socket, addr = self.server_socket.accept()
            if addr not in connected_data_nodes:
                connected_data_nodes.add(addr)

                files = self.partitions.pop()
                client_socket.sendall(str.encode(str(files)))
                print('[NameNode]: Enviando instrucciones a DataNode: '+str(addr))

            client_socket.close()
        
        # print confirmation
        print('[NameNode]: Todos los DataNodes conectados')
        self.server_socket.close()
        
        self.map_reduce()
        

    
    def map_reduce(self):
        print('[NameNode]: Esperando subresults...')
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        host = self.config['name_node']['host']
        port = self.config['name_node']['port']
        print('[NameNode]: Iniciando servidor mapreduce en '+str(host)+':'+str(port))
        self.server_socket.bind((host, port))

        self.server_socket.listen(1)

        while True:
            client_socket, addr = self.server_socket.accept()
            subresult = client_socket.recv(1024)
            print('[NameNode]: Recibiendo subresultados de DataNode: '+str(addr))
            self.main_result = reducer(self.main_result, eval(subresult.decode('utf-8')))
            client_socket.close()
           
            # store result in a file
            with open(self.config['result_path'] + '/result.txt', 'w', encoding='utf-8') as f:
                f.write(str(self.main_result))

        self.server_socket.close()
