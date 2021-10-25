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
    for i in range(num_server):
        clients_list[i] = config['client_list'][str(i)]
        
    server_list = {}
    for i in range(num_server):
        server_list[i] = config['server_list'][str(i)]
    quorum = num_server // 2 + 1

    request_queue = Queue() #(client_id,request_info)
    view = 0
    proposal_id = 0
    
    drop_rate = config['drop_rate']
    proposer = Proposer(server_id, server_list, drop_rate, config['skip'], config['num_failed_primary'])
    acceptor = Acceptor(server_id, server_list, drop_rate, view)
    learner = Learner(server_id, server_list, drop_rate, clients_list)
    sender_ = Sender(drop_rate)
    
    isLeader = False
    viwechange_log = {}
    
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
    while True: 
        print("server start", server_id)
        conn, addr = s.accept()
        data = conn.recv(4096*2)
        msg = pickle.loads(data)
        print("msg:", msg)
        
        # if msg['type'] == 'REQUEST':
        #     print("request", msg)
        #     if view % num_server == server_id:
        #         # testcase2, 3
        #         if num_failed_primary > 0 and server_id < num_failed_primary:
        #             view += 1
        #             request_queue = Queue() #new leader clears the request queue 
        #             proposer.have_prepared = False
        #             exit()
        #         request_queue.put((msg['client_id'], msg['request_info']))
        #         print("request_queue:", request_queue)
        #         if not proposer.have_prepared:
        #             proposer.prepare(proposal_id)
        #             proposer.have_prepared = True
        #             proposal_id += 1
        #         else:  #directly propose without prepare stage
        #             for _ in range(request_queue.qsize()):
        #                 client_id, request_info = request_queue.get()
        #                 print("propose:",{"request_info": request_info, "client_id": client_id})
        #                 proposer.propose({"request_info": request_info, "client_id": client_id})
        #                 proposal_id += 1
        
        
        if msg['type'] == 'REQUEST':
            print("request", msg)
            if isLeader:
                # testcase2, 3
                if num_failed_primary > 0 and server_id < num_failed_primary:
                    exit()
                proposer.backup_request(msg)
            elif msg['resend_id'] > 0:
                new_view = view + 1
                msg2 = {
                    'type' : 'VIEWCHANGE',
                    'new_view' : new_view,
                    'replica_id' : server_id
                }
                host, port = server_list[new_view % num_server]['host'], server_list[new_view % num_server]['port']
                sender_.send(host, port, msg2)
                            

        # elif msg['type'] == 'PROMISE':
        #     print("promise", msg)
        #     proposer.process_promise(msg)
        #     print("proposer.message_promise:", proposer.message_promise)
        #     print("proposer.have_prepared:", proposer.have_prepared)
        #     print("proposer.count_acceptor:", proposer.count_acceptor)
        #     if len(proposer.count_acceptor) >= proposer.quorum:
        #         if not proposer.have_prepared:
        #             for _ in range(request_queue.qsize()):
        #                 client_id, request_info = request_queue.get()
        #                 proposer.propose({"request_info": request_info, "client_id": client_id})
        #                 proposer.have_prepared = True
        #     print("proposer.message_promise:", proposer.message_promise)
        
        elif msg['type'] == 'PROMISE':
            # I'm going to become the new leader, preparing for propose
            if msg['proposal_id'] % num_server == server_id:
                proposer.process_promise(msg)
                """
                getting a majority of promise
                change isLeader to True after accepted by majority
                process the received acceptance logs to build up the profile
                re-propose everything up to highest slot_num
                send to client: I'm new leader
                """
                if not isLeader and len(proposer.count_acceptor) >= proposer.quorum:
                    print(u'{} becomes the new leader, proposing now'.format(msg['proposal_id']))
                    isLeader = True
                    # notify clients viewchange
                    for c in clients_list:
                        sender_.send(c['host'], c['port'], {'type': 'VIEWCHANGE'})
                    proposal_list = proposer.get_proposals()
                    for slot_idx, proposal in proposal_list.items():
                        proposer.propose(slot_idx, proposal)

        # elif msg['type'] == 'PREPARE':
        #     print("PREPARE", msg)
        #     view = max(view, msg['proposal_id'])  # try to catch up with the most recent view
        #     acceptor.promise(msg)
        
        elif msg['type'] == 'PREPARE':
            if msg['proposal_id'] < view:
                return
            print(u'New leader {} want to prepare: {}'.format(msg['proposal_id'], str(msg)))
            # I receive a view change confirm, help new primary to prepare
            view = msg['proposal_id']
            acceptor.current_proposal_id = view
            if isLeader and view % num_server == server_id:
                isLeader = False
            print("line 154,", msg)
            acceptor.promise(msg)
            
        # elif msg['type'] == 'PROPOSE':
        #     print("propose", msg)
        #     acceptor.accept(msg)

        elif msg['type'] == 'PROPOSE':
            print("propose", msg)
            if view < msg['proposal_id']:
                # if I'm in an older view, I must lost view change message, update view now
                print("View changed by: " + str(msg))
                view = msg['proposal_id']
                if isLeader:
                    isLeader = False
                acceptor.promised_proposal_id = view
            print("line 170,", msg)
            acceptor.accept(msg)


        # elif msg['type'] == 'ACCEPT':
        #     print("accept", msg)
        #     slot_idx = msg['slot']
        #     learner.add_accept(msg)
        #     flag = learner.majority_have_accepted(msg['proposal_id'], slot_idx)
        #     print("===---===",flag)
        #     if flag:
        #         learner.decide(msg['proposal_id'], slot_idx)
                
        elif msg['type'] == 'ACCEPT':
            print("accept", msg)
            learner.add_accept(msg)
            # learner gets ACCEPT message from acceptors
            k = learner.majority_have_accepted(msg['proposal_id'], msg['slot'])
            print(msg, "++++++++++++",k)
            if k is not False:
                print("majority_have_accepted")
                # decided this slot and check execution
                print("======decide", msg)
                learner.decide(msg['proposal_id'], msg['slot'], k)
                print("======execute")
                learner.execute()

        elif msg['type'] == 'VIEWCHANGE':
            if msg['new_view'] < view:
                return
            new_view = msg['new_view']
            
            process_viewchange = False
            proposer_rid = msg['replica_id']
            if new_view % num_server == server_id:
                process_viewchange = False
            if new_view < view:
                print("My current view is", view, "but giving me view change")
                process_viewchange = False
            if new_view in viwechange_log:
                viwechange_log[new_view][proposer_rid] = True
            else:
                viwechange_log[new_view] = {}
                viwechange_log[new_view][proposer_rid] = True
            print("Getting",len(viwechange_log[new_view]),"votes")
            if len(viwechange_log[new_view]) >= quorum:
                process_viewchange = True
            process_viewchange = False
    
            # other replica want me to become leader
            if process_viewchange(msg) and not isLeader:
                # a majority of replica want me to become leader
                view = msg['new_view']
                acceptor.current_proposal_id = view
                print("I'm the new leader by majority: " + str(msg))
                proposer.message_promise = {}
                proposer.count_acceptor = []
                proposer.prepare(msg['new_view'])

                      
        conn.close()
    

           

if __name__ == "__main__":

    # server(int(sys.argv[1]), sys.argv[2])
    from optparse import OptionParser, OptionGroup

    parser = OptionParser(usage = "Usage!")
    options, args = parser.parse_args()
    options = dict(options.__dict__)

    server(*args, **options)