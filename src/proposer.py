import socket
import pickle
from sender import Sender

class Proposer(object):
    def __init__(self, server_id, server_list, loss):
        self.server_id = server_id
        self.acceptor_list = server_list    #{"id": {"host": "xxx", "port": xxx}}
        self.loss = loss
        self.quorum = len(server_list) // 2 + 1
        self.sender = Sender(self.loss)
        # self.have_prepared = False
        self.proposal_id = None
        self.latest_proposal_id = None
        self.count_acceptor = []    #[acceptor_id]
        self.message_promise = {} #{proposal_id: message{}}
        self.available_slot = 0
        
    def prepare(self):
        msg = {
            "type": "PREPARE",
            "proposal_id": self.proposal_id,
            "server_id": self.server_id
        }
        self.latest_proposal_id = self.proposal_id
        for k, v in self.acceptor_list.items():
            self.sender.send(v["host"], v["port"], msg)
        
    def process_promise(self, msg):
        acceptor_id = msg["acceptor_id"]
        proposal_id = msg["proposal_id"]
        if self.proposal_id != proposal_id:
            return
        self.count_acceptor.append(acceptor_id)
        previous_proposal_id = msg["previous_proposal_id"]
        self.message_promise[previous_proposal_id] = msg

