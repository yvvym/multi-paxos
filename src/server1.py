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
    
    server_list = {}
    for i in range(num_server):
        server_list[i] = config['server_list'][i]
    quorum = num_server // 2 + 1

    drop_rate = config['drop_rate']
    proposer = Proposer(server_id, server_list, drop_rate)
    acceptor = Acceptor(server_id, server_list, drop_rate)
    # learner = Learner(server_id, quorum, drop_rate)

    HOST = server_list[server_id]['host']       
    PORT = server_list[server_id]['port']      
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen(100)

    request_queue = Queue() #(client_id,request_info)
    view = 0

    while True: 
        conn, addr = s.accept()
        data = conn.recv(4096*2)
        msg = pickle.loads(data)
        if msg['type'] == 'REQUEST':
            if msg['resend_id'] > 0:
                view += 1

                #new leader clears the request queue 
                request_queue = Queue()

                proposer.need_prepare = True

            if view % num_server == server_id:
                request_queue.put((msg['client_id'], msg['request_info']))

            if proposer.have_prepared:
                proposer.prepare(view)
            else: #directly propose without prepare stage
                proposal_pack = {}
                    
                for _ in range(len(request_queue)):
                    client_id, request_info = request_queue.get()
                    # proposal_pack = proposer.addNewRequest(proposal_pack, request_info, client_id)
        
                    proposer.propose(proposal_pack, without_prepare = True)

        elif msg['type'] == 'PROMISE':
            proposer.addVote(msg)
            if proposer.proposal_id in proposer.proposal_count and len(proposer.proposal_count[proposer.proposal_id]) >= proposer.quorum:
                if proposer.have_prepared:
                    for _ in range(len(request_queue)):
                        client_id, request_info = request_queue.get()
                        # proposal_pack = proposer.addNewRequest(proposal_pack, request_info, client_id) 
                    proposer.propose(proposal_pack)
                proposer.have_prepared = False

        elif msg['type'] == 'PREPARE':
            view = max(view, msg['proposal_id'])  # try to catch up with the most recent view
            acceptor.promise(msg)
            
        elif msg['type'] == 'propose':
            acceptor.accept(msg)

        # elif msg['type'] == 'accept':
        #     slot_idx = msg['slot_idx']
        #     learner.addVote(msg, slot_idx)
        #     if learner.checkQuorumSatisfied(slot_idx):
        #         learner.decide(slot_idx)
                      
                

        conn.close()
    

           

if __name__ == "__main__":

    server(int(sys.argv[1]), sys.argv[2])