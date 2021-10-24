import socket
import pickle
import json
import sys

from sender import Sender

timeout = 20

def client(client_id, config_file = '../config/testcase1.json'):
    host_name = 'localhost'
    clients_list = {idx:{'host': host_name, 'port': 40000+idx} for idx in range(10)} 
    #load config file
    f =  open(config_file, 'r')
    config = json.load(f)
    
    num_server = int(config['num_server']) #number of servers
    
    server_list = {}
    for i in range(num_server):
        server_list[i] = config['server_list'][i]
    quorum = num_server // 2 + 1

    drop_rate = config['drop_rate']
    
    sender_ = Sender(drop_rate)
    
    client_id = int(client_id)
    client_host = clients_list[client_id]['host']
    client_port = clients_list[client_id]['port']

    request_message = ['apple', 'orange', 'banana', 'pear', 'lemon']
    # request_list = ['client, seq: (%s, %s)'%(str(client_id), request_message[i] ) for i in range(len(request_message)) ]
    
    
    for i in range(len(request_message)):
        val = request_message[i]
        resend_idx = 0
        while True:
            client_info = { 'request_id': i, 'client_id': client_id, 'client_host': client_host, 'client_port': client_port }
            msg = {'type': 'request', 'request_val': val, 'resend_idx': resend_idx, 'client_info': client_info}
            for server_id in server_list:
                host = server_list[server_id]['host']
                port = server_list[server_id]['port']
    
                # send msg to (host, port)
                sender_.send(host, port, msg)
            
            if wait_ack(client_host, client_port, timeout, i):
               break

            resend_idx += 1


def wait_ack(client_host, client_port, timeout, clt_seq_num):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((client_host, client_port))
    s.listen(100)
    
    while True:
       s.settimeout(timeout)
       conn, addr = s.accept()
       data = conn.recv(4096*2)
       msg = pickle.loads(data)
       conn.close()

       #wait for the right clt_seq_num
       if msg['type'] == 'ack' and msg['client_info']['clt_seq_num'] == clt_seq_num:
           return True
    

if __name__ == "__main__":
    client(int(sys.argv[1]), sys.argv[2])