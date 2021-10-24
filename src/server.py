import socket
import pickle
from queue import Queue
import json
import os
import sys

from proposer import Proposer
from acceptor import Acceptor
from learner import Learner

def server(server_id, config_file = '../config/testcase1.json'):
    server_id = int(server_id)

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
    quorum = num_server // 2 + 1

    drop_rate = config['drop_rate']
    proposer = Proposer(server_id, server_list, drop_rate)
    acceptor = Acceptor(server_id, server_list, drop_rate)
    learner = Learner(server_id, server_list, drop_rate, clients_list)

    HOST = server_list[server_id]['host']       
    PORT = server_list[server_id]['port']      
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen(100)

    request_queue = Queue() #(client_id,request_info)
    view = 0
    proposal_id = 0
    
    num_failed_primary = int(config['num_failed_primary'])

    while True: 
        print("server start", server_id)
        conn, addr = s.accept()
        data = conn.recv(4096*2)
        msg = pickle.loads(data)
        print("msg:", msg)
        if msg['type'] == 'REQUEST':
            print("request", msg)
            if view % num_server == server_id:
                
                # testcase2, 3
                if num_failed_primary > 0 and server_id < num_failed_primary:
                    view += 1
                    request_queue = Queue() #new leader clears the request queue 
                    proposer.have_prepared = False
                    exit()
                
                request_queue.put((msg['client_id'], msg['request_info']))
                print("request_queue:", request_queue)

            print("request_queue.qsize():",request_queue.qsize())
            if not proposer.have_prepared:
                proposer.prepare(proposal_id)
                proposal_id += 1
            else:  #directly propose without prepare stage
                for _ in range(request_queue.qsize()):
                    client_id, request_info = request_queue.get()
                    print("propose:",{"request_info": request_info, "client_id": client_id})
                    proposer.propose({"request_info": request_info, "client_id": client_id})
                    proposal_id += 1
                    

        elif msg['type'] == 'PROMISE':
            print("promise", msg)
            proposer.process_promise(msg)
            if proposer.proposal_id in proposer.message_promise and len(proposer.count_acceptor) >= proposer.quorum:
                if not proposer.have_prepared:
                    for _ in range(len(request_queue)):
                        client_id, request_info = request_queue.get()
                        proposer.propose({"request_info": request_info, "client_id": client_id})
                        proposer.have_prepared = True

        elif msg['type'] == 'PREPARE':
            print("PREPARE", msg)
            view = max(view, msg['proposal_id'])  # try to catch up with the most recent view
            acceptor.promise(msg)
            
        elif msg['type'] == 'propose':
            print("propose", msg)
            acceptor.accept(msg)

        elif msg['type'] == 'accept':
            print("accept", msg)
            slot_idx = msg['slot_idx']
            learner.add_accept(msg)
            if learner.majority_have_accepted(msg['proposal_id'], slot_idx):
                learner.decide(msg['proposal_id'], slot_idx)

                      
        conn.close()
    

           

if __name__ == "__main__":

    # server(int(sys.argv[1]), sys.argv[2])
    from optparse import OptionParser, OptionGroup

    parser = OptionParser(usage = "Usage!")
    options, args = parser.parse_args()
    options = dict(options.__dict__)

    server(*args, **options)