import socket
import pickle
from queue import Queue
import json
import os
import sys

from proposer import Proposer
from acceptor import Acceptor
from learner import Learner
from sender import Sender

def server(server_id, config_file = '../config/testcase1.json'):
    server_id = int(server_id)
    #load config file
    f =  open(config_file, 'r')
    config = json.load(f)

    num_server = int(config['num_server']) #number of servers
    
    clients_list = {}
    for i in range(len(list(config['client_list'].keys()))):
        clients_list[i] = config['client_list'][str(i)]
        
    server_list = {}
    for i in range(num_server):
        server_list[i] = config['server_list'][str(i)]
    quorum = num_server // 2 + 1

    view = 0
    
    drop_rate = config['drop_rate']
    proposer = Proposer(server_id, server_list, drop_rate, config['skip'], config['num_failed_primary'])
    acceptor = Acceptor(server_id, server_list, drop_rate, view)
    learner = Learner(server_id, server_list, drop_rate, clients_list)
    sender_ = Sender(drop_rate)
    
    isLeader = False
    
    HOST = server_list[server_id]['host']       
    PORT = server_list[server_id]['port']      
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen(100)

    
    if view % num_server == server_id:
        isLeader = True
    else:
        isLeader = False
    
    num_failed_primary = int(config['num_failed_primary'])
    failed_primary = config['failed_primary']
    
    while True: 
        print("server start", server_id)
        conn, addr = s.accept()
        data = conn.recv(4096*2)
        msg = pickle.loads(data)
        
        if msg['type'] == 'REQUEST':
            print("request", msg)
            if isLeader:
                # testcase2, 3
                if num_failed_primary > 0 and server_id in failed_primary:
                    print("force the primary %s to crash"%(str(server_id)))
                    new_view = view + 1
                    msg2 = {
                        'type' : 'VIEWCHANGE',
                        'new_view' : new_view,
                        'replica_id' : server_id
                    }
                    for i in range(new_view % num_server, num_server):
                        host, port = server_list[i]['host'], server_list[i]['port']
                        sender_.send(host, port, msg2)
                    exit()
                proposer.backup_request(msg)
        
        elif msg['type'] == 'PROMISE':
            if msg['proposal_id'] % num_server == server_id: #new leader
                print("line133", isLeader)
                proposer.process_promise(msg)
                print(str(msg['proposal_id']) + 'becomes the new leader')
                isLeader = True
                for i in list(clients_list.items()):
                    sender_.send(i[1]['host'], i[1]['port'], {'type': 'VIEWCHANGE'})
                proposal_list = proposer.get_proposals()
                if proposal_list is not False:
                    for slot_idx, proposal in proposal_list.items():
                        proposer.propose(slot_idx, proposal)

        
        elif msg['type'] == 'PREPARE':
            if msg['proposal_id'] < view:
                return
            print('New leader:' + str(msg['proposal_id']) + ', msg:' + str(msg))
            view = msg['proposal_id']
            acceptor.current_proposal_id = view
            if isLeader and view % num_server == server_id:
                isLeader = False
            acceptor.promise(msg)

        elif msg['type'] == 'PROPOSE':
            print("propose", msg)
            if view < msg['proposal_id']:
                print("view changed: " + str(msg))
                view = msg['proposal_id']
                if isLeader:
                    isLeader = False
                acceptor.promised_proposal_id = view
            acceptor.accept(msg)
                
        elif msg['type'] == 'ACCEPT':
            learner.add_accept(msg)
            k = learner.majority_have_accepted(msg['proposal_id'], msg['slot'])
            if k is not False:
                print("majority_have_accepted")
                learner.decide(msg['proposal_id'], msg['slot'], k)
                learner.execute(config['skip'])

        elif msg['type'] == 'VIEWCHANGE':
            if msg['new_view'] < view:
                return
            new_view = msg['new_view']
            process_viewchange = True
    
            if process_viewchange and not isLeader:
                view = msg['new_view']
                acceptor.promised_proposal_id = view
                proposer.message_promise = {}
                proposer.count_acceptor = []
                proposer.prepare(msg['new_view'])

        elif msg['type'] == 'MISS':
            learner.reply_missed_decided_value(msg)
            
        elif msg['type'] == 'REPLY':
            learner.process_reply(msg)
                     
              
        conn.close()
    

           

if __name__ == "__main__":
    from optparse import OptionParser, OptionGroup

    parser = OptionParser(usage = "Usage!")
    options, args = parser.parse_args()
    options = dict(options.__dict__)

    server(*args, **options)