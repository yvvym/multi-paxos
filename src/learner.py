from acceptor import Acceptor
from sender import Sender

class Learner(object):
    def __init__(self, server_id, server_list, loss, client_list):
        self.server_id = server_id
        self.loss = loss
        self.client_list = client_list
        self.quorum = len(server_list) // 2 + 1
        self.proposal_list = {} #{proposal_id:{"request_info":xxx, "client_id":xxx}}
        self.decided_log = {}   #{slot:proposal_id}
        self.executed_log = {}  #{slot:proposal_id}
        self.slots = {} #{slot:{proposal_id:[acceptors]}}
    
    def add_accept(self, msg):
        acceptor_id = msg["acceptor_id"]
        slot = msg["slot"]
        request_info = msg["request_info"]
        client_id = msg["client_id"]
        proposal_id = msg["proposal_id"]
        if proposal_id not in self.slots[slot]:
            self.slots[slot][proposal_id] = {}
            self.slots[slot][proposal_id]["request_info"] = request_info
            self.slots[slot][proposal_id]["client_id"] = client_id
            self.slots[slot][proposal_id]["acceptors"] = []
        self.slots[slot][proposal_id]["acceptors"].append(acceptor_id)

    def majority_have_accepted(self, proposal_id, slot):
        count = len(self.slots[slot][proposal_id]["acceptors"])
        if count >= self.quorum:
            return True
        else:
            return False
        
    def decide(self, proposal_id, slot):
        self.decided_log[slot]["request_info"] = 


    def execute(self):

    
        