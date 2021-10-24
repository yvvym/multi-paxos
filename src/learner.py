from acceptor import Acceptor
from sender import Sender
import logging

class Learner(object):
    def __init__(self, server_id, server_list, loss, client_list):
        self.server_id = server_id
        self.loss = loss
        self.client_list = client_list
        self.quorum = len(server_list) // 2 + 1

        self.file_logger = logging.getLogger(str(server_id))
        self.file_logger.setLevel(logging.INFO)
        # ch = logging.StreamHandler()
        log_file = '../log/{}.log'.format(server_id)
        self.execute_file = '../exe/{}.txt'.format(server_id)
        fh = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
        fh.setFormatter(formatter)
        self.file_logger.addHandler(fh)

        self.proposal_list = {} #{proposal_id:{"request_info":xxx, "client_id":xxx}}
        self.decided_log = {}   #{slot:proposal_id}
        self.executed_log = {}  #{slot:proposal_id}
        self.slots = {} #{slot:{proposal_id:[acceptors]}}
        self.slot_to_execute = 0
    
    def add_accept(self, msg):
        acceptor_id = msg["acceptor_id"]
        slot = msg["slot"]
        request_info = msg["request_info"]
        client_id = msg["client_id"]
        proposal_id = msg["proposal_id"]
        if proposal_id not in self.slots[slot]:
            self.slots[slot][proposal_id] = proposal_id
            self.proposal_list[proposal_id]["request_info"] = request_info
            self.proposal_list[proposal_id]["client_id"] = client_id
            self.slots[slot][proposal_id] = []
        self.slots[slot][proposal_id].append(acceptor_id)

    def majority_have_accepted(self, proposal_id, slot):
        count = len(self.slots[slot][proposal_id])
        if count >= self.quorum:
            return True
        else:
            return False
        
    def decide(self, proposal_id, slot):
        self.decided_log[slot] = proposal_id
        decided_request_info = self.proposal_list[proposal_id]["request_info"]
        decided_client_id = self.proposal_list[proposal_id]["client_id"]
        log = "learner " + str(self.server_id) + " decided slot "+ str(slot) + " and request " + decided_request_info + " by client " + str(decided_client_id)
        self.file_logger.info(log)
        msg = {
            "type": "ack",
            "val": decided_request_info,
            "client_info": decided_client_id
        }
        for k, v in self.client_list.items():
            self.sender.send(v["host"], v["port"], msg)



    def execute(self):
        while self.slot_to_execute in self.decided_log:
            proposal_id = self.decided_log[self.slot_to_execute]
            self.executed_log[self.slot_to_execute] = proposal_id
            exe = str(self.proposal_list[proposal_id]["client_id"]) + " " + self.proposal_list[proposal_id]["request_info"]
            with open(self.execute_file, 'a') as f:
                f.write(exe)
            self.slot_to_execute += 1

    
        