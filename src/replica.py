from proposer import Proposer
from acceptor import Acceptor
from learner import Learner

from collections import deque

class Replica:
    def __init__(self, f, rid, skip, fail, replicas, clients, log_file, loss_rate=0, debug=True):
        self.f=f
        self.rid=rid
        self.skip = skip
        self.replica_list = replicas
        self.fail = fail # num of forced crash replicas, for test 2, 3
        self.total_p=2*f+1
        self.majority=f+1
        self.acceptor=Acceptor(self.rid, 0)
        self.proposer=Proposer(self.f, self.rid, self.skip)
        self.learner=Learner(self.f, self.rid, loss_rate)
        self.view=0 # current view num
        self.processed_request={} # request is added when replied to client
        self.viwechange_log={}
        self.client_list = clients # client hosts ports
        self.slot_num = 0
        self.loss_rate = loss_rate
        self.isLeader = False
        self.is_live = True
        self.log_file = log_file # log_file path
        self.setLeader()

        print("Setting up socket")
        # set up receiving socket
        self.host = replicas[rid].host
        self.port = replicas[rid].port
        self.timeout = PROCESS_TIMEOUT
        self.s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.s.bind((self.host, self.port))
        self.s.settimeout(self.timeout)
        print("Socket set")

    def setLeader(self):
        if self.view % self.total_p == self.rid:
            self.isLeader = True
        else:
            self.isLeader = False

    def run(self):
        while True:
            try:
                receive(self.s, self.handle_msg)
            except KeyboardInterrupt:
                return
            except socket.timeout:
                print("waiting for message")


    def handle_msg(self, msg):
        print("Handling message", msg)

        if msg['type'] == ACCEPT:
            # learner gets ACCEPT message from acceptors
            if self.learner.process_accept(msg):
                # if self.debug:
                #     self.debug_log.write
                # print("Learner decided: " + str(msg))
                # decided this slot and check execution
                self.learner.decide(msg['slot_num'], msg['proposal_id'])
                if not self.learner.execute(self.log_file):
                    self.learner.query_others(self.replica_list, self.loss_rate)
                    # self.propose_viewchange()


        elif msg['type'] == PROPOSE:
            # acceptor get PROPOSE message from proposer
            print("Get Decree:", msg)
            if self.view < msg['proposal_id']:
                # if I'm in an older view, I must lost view change message, update view now
                # if self.debug:
                #     self.debug_log.write
                print("View changed by: " + str(msg))
                self.view = msg['proposal_id']
                if self.isLeader:
                    self.isLeader = False
                self.acceptor.current_proposal_id = self.view
                self.acceptor.current_proposer_id = self.view % self.total_p

            self.acceptor.process_proposal(msg, self.replica_list, self.loss_rate)

        elif msg['type'] == REQUEST:
            if self.isLeader:
                # testcase 2 and 3
                if self.fail > -1 and self.rid < self.fail:
                    logging.info("force the primary %s to crash"%(str(self.rid)))
                    logging.info("server id %s crashes"%(str(self.rid)))
                    exit()
                # leader propose value
                # print("Proposing for this request: " + str(msg))
                self.proposer.propose_request(msg, self.replica_list, self.loss_rate)
            elif msg['resend_id'] > 0:
                # all replica check resend_id, if resend_id > 0 trigger view change
                print("Viewchanging for this timeout request: " + str(msg))
                self.propose_viewchange()
                
        elif msg['type'] == VIEWCHANGE:
            if msg['new_view'] < self.view:
                return
            # other replica want me to become leader
            if self.process_viewchange(msg) and not self.isLeader:
                # a majority of replica want me to become leader
                self.view = msg['new_view']
                self.acceptor.current_proposal_id = self.view
                self.acceptor.current_proposer_id = self.view % self.total_p
                print("I'm the new leader by majority: " + str(msg))
                self.proposer.msg_log={}
                self.proposer.acc_counter={}
                self.proposer.prepare(msg['new_view'], self.replica_list, self.loss_rate)

        elif msg['type'] == PREPARE:
            if msg['proposal_id'] < self.view:
                return
            print(u'New leader {} want to prepare: {}'.format(msg['proposal_id'], str(msg)))
            # I receive a view change confirm, help new primary to prepare
            self.view = msg['proposal_id']
            self.acceptor.current_proposal_id = self.view
            self.acceptor.current_proposer_id = self.view % self.total_p
            if self.isLeader and self.view % self.total_p != self.rid:
                self.isLeader = False
            self.acceptor.promise(msg, self.replica_list, self.loss_rate)
        
        elif msg['type'] == PROMISE:
            # I'm going to become the new leader, preparing for propose
            if msg['proposal_id'] % self.total_p == self.rid:
                    self.proposer.addAcceptence(msg)
                    """
                    getting a majority of promise
                    change isLeader to True after accepted by majority
                    process the received acceptance logs to build up the profile
                    re-propose everything up to highest slot_num
                    send to client: I'm new leader
                    """
                    if not self.isLeader and self.proposer.isAcceptedByQuorum():
                        print(u'{} becomes the new leader, proposing now'.format(msg['proposal_id']))
                        self.isLeader = True
                        # notify clients viewchange
                        for c in self.client_list:
                            send(c.host, c.port, {'type': VIEWCHANGE}, self.loss_rate)
                        proposal_list = self.proposer.getProposalList()
                        for slot_idx, proposal in proposal_list.items():
                            logging.info('leader %s propose %s', str(self.view), str(slot_idx))
                            self.proposer.propose(slot_idx, proposal, self.replica_list, self.loss_rate)


        elif msg['type'] == QUERY:
            # other learners ask me about they don't have slot_num N
            if not self.learner.process_query(msg, self.replica_list, self.loss_rate):
                print("I dont know about this query, proposing view change: " + str(msg))
                # I also don't have it
                self.propose_viewchange()

        elif msg['type'] == RESPOND:
            if msg['slot_num'] not in self.learner.decide_log:
                print("Others tell me about this learn: " + str(msg))
                if not self.learner.learn(msg, self.log_file):
                    # if stock on some slots, query others first
                    self.learner.query_others(self.replica_list, self.loss_rate)

                
        print("Finish message", msg)

    def propose_viewchange(self):
        new_view = self.view+1
        msg = {
            'type' : VIEWCHANGE,
            'new_view' : new_view,
            'replica_id' : self.rid
        }
        host, port = self.replica_list[new_view % self.total_p].host, self.replica_list[new_view % self.total_p].port
        send(host, port, msg, self.loss_rate)

    def process_viewchange(self, msg):
        new_view = msg['new_view']
        proposer_rid = msg['replica_id']
        if new_view % self.total_p != self.rid:
            return False
        if new_view<self.view:
            print("My current view is", self.view, "but giving me view change")
            return False
        if new_view in self.viwechange_log:
            self.viwechange_log[new_view][proposer_rid] = True
        else:
            self.viwechange_log[new_view]={}
            self.viwechange_log[new_view][proposer_rid] = True
        
        print("Getting",len(self.viwechange_log[new_view]),"votes")
        if len(self.viwechange_log[new_view])>=self.majority:
            return True
        
        return False

        
