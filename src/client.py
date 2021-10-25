import socket
import pickle
import json
import sys

from sender import Sender

timeout = 20

def client(client_id, config_file = '../config/testcase1.json'):
    #load config file
    f =  open(config_file, 'r')
    config = json.load(f)

    num_server = int(config['num_server']) #number of servers
    clients_list = {}
    for i in range(num_server):
        clients_list[i] = config['client_list'][str(i)]

    server_list = {}
    for i in range(num_server):
        server_list[i] = config['server_list'][str(i)]

    drop_rate = config['drop_rate']
    
    sender_ = Sender(drop_rate)
    
    client_id = int(client_id)
    client_host = clients_list[client_id]['host']
    client_port = clients_list[client_id]['port']
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((client_host, client_port))
    s.listen(100)
    # s.settimeout(timeout)

    request_message = ['apple', 'orange', 'banana', 'pear', 'lemon']
    
    # for i in range(len(request_message)):
    #     val = request_message[i]
    #     print("||||||||||||||||||||",val)
    #     while True:
    #         msg = {'type': 'REQUEST', 'client_id': client_id, 'request_info': val, 'resend_id': 0, 'client_request_id': i}
    #         for server_id in server_list:
    #             host = server_list[server_id]['host']
    #             port = server_list[server_id]['port']
    
    #             # send msg to (host, port)
    #             sender_.send(host, port, msg)
            
    #         if wait_ack(client_host, client_port, timeout, i, s) == 'ACK':
    #             break
    #         elif wait_ack(client_host, client_port, timeout, i, s) == 'VIEWCHANGE':
    #             print("VIEWCHANGE")
    #             msg['resend_id'] = 0
    #         elif wait_ack(client_host, client_port, timeout, i, s) == 'TIMEOUT':
    #             print("TIMEOUT")
    #             msg['resend_id'] += 1
    #         else:
    #             print("ACK ERROR")
    
    val = request_message[client_id]
    print("||||||||||||||||||||",val)
    while True:
        msg = {'type': 'REQUEST', 'client_id': client_id, 'request_info': val, 'client_request_id': client_id}
        for server_id in server_list:
            host = server_list[server_id]['host']
            port = server_list[server_id]['port']
    
            # send msg to (host, port)
            sender_.send(host, port, msg)
            
        if wait_ack(client_host, client_port, timeout, i, s) == 'ACK':
            break
        elif wait_ack(client_host, client_port, timeout, i, s) == 'VIEWCHANGE':
            print("VIEWCHANGE")
        elif wait_ack(client_host, client_port, timeout, i, s) == 'TIMEOUT':
            print("TIMEOUTTTTTTTTTTTTTTTTTT")
        else:
            print("ACK ERROR")
            exit()


def wait_ack(client_host, client_port, timeout, client_id, s):
    print("wait_ack")
    # s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # s.bind((client_host, client_port))
    # s.listen(100)
    
    while True:
        s.settimeout(timeout)
        try: 
            conn, addr = s.accept()
        except socket.timeout:
            return ""
        
        try:
            data = conn.recv(4096*2)
        except socket.timeout:
            return 'TIMEOUT'
        msg = pickle.loads(data)
        conn.close()
        
        print("=========line77", msg)
        #wait for the right client_id
        if msg['type'] == 'ACK' and msg['client_info'] == client_id:
            return 'ACK'
        elif msg['type'] == 'VIEWCHANGE':
            return 'VIEWCHANGE'
            
       
    

if __name__ == "__main__":
    # client(int(sys.argv[1]), sys.argv[2])
    from optparse import OptionParser, OptionGroup

    parser = OptionParser(usage = "Usage!")
    options, args = parser.parse_args()
    options = dict(options.__dict__)

    client(*args, **options)