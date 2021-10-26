from acceptor import Acceptor
from sender import Sender
import logging

class Learner(object):
    def __init__(self, server_id, server_list, loss, client_list):
        self.server_id = server_id
        self.loss = loss
        self.client_list = client_list
        self.server_list = server_list
        self.quorum = len(server_list) // 2 + 1
        self.sender = Sender(loss)

        self.file_logger = logging.getLogger(str(server_id))
        self.file_logger.setLevel(logging.INFO)
        # ch = logging.StreamHandler()
        log_file = '../log/{}.log'.format(server_id)
        self.execute_file = '../exe/{}.txt'.format(server_id)
        fh = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
        fh.setFormatter(formatter)
        self.file_logger.addHandler(fh)
        self.decided_log = {}   #{slot:{"proposal_id":xxx, "request_info": xxx, "client_id":xxx, "client_request_id":xxx}}
        self.executed_log = {}  #{slot:{"proposal_id":xxx, "request_info": xxx, "client_id":xxx, "client_request_id":xxx}}
        self.slots = {} #{slot:{proposal_id:{(request_info, client_id, client_request_info): [acceptor_id]}}}
        self.slot_to_execute = 0
    
    def add_accept(self, msg):
        print("add_accept")
        acceptor_id = msg["acceptor_id"]
        slot = msg["slot"]
        request_info = msg["request_info"]
        client_id = msg["client_id"]
        client_request_id = msg["client_request_id"]
        proposal_id = msg["proposal_id"]
        if slot not in self.slots:
            self.slots[slot] = {}
        if proposal_id not in self.slots[slot]:
            self.slots[slot][proposal_id] = {}
        value = (request_info, client_id, client_request_id)
        print("value:", value)
        if value not in self.slots[slot][proposal_id]:
            self.slots[slot][proposal_id][value] = []
        self.slots[slot][proposal_id][value].append(acceptor_id)
        self.slots[slot][proposal_id][value] = list(set(self.slots[slot][proposal_id][value]))
        print("self.slots:", self.slots)
            

    def majority_have_accepted(self, proposal_id, slot):
        print("slot",slot,self.slots)
        print("proposal_id", proposal_id)
        if slot not in self.slots:
            return False
        print("self.slots[slot][proposal_id]:",self.slots[slot][proposal_id])
        slot_list = list(self.slots[slot][proposal_id].items())
        print("slot_list:", slot_list)
        for i in slot_list:
            k = i[0]
            v = i[1]
            if len(v) >= self.quorum:
                return k    #return (request_info, client_id, client_request_id) which will be decided
        return False
        
    def decide(self, proposal_id, slot, k):
        print("decide")
        decided_request_info, decided_client_id, decided_client_request_id = k
        if slot in self.decided_log:
            return
        self.decided_log[slot] = {}
        self.decided_log[slot]["request_info"] = decided_request_info
        self.decided_log[slot]["client_id"] = decided_client_id
        self.decided_log[slot]["client_request_id"] = decided_client_request_id
        if decided_request_info != "NOOP":
            log = "learner " + str(self.server_id) + " decided slot "+ str(slot) + " with request " + str(decided_client_request_id) + " by client " + str(decided_client_id) + ": " + decided_request_info
            self.file_logger.info(log)
            msg = {
                "type": "ACK",
                "val": decided_request_info,
                "client_info": decided_client_id
            }
            self.sender.send(self.client_list[decided_client_id]["host"], self.client_list[decided_client_id]["port"], msg)


    def execute(self, skip_slot):
        print("execute")
        print("self.slot_to_execute:",self.slot_to_execute)
        print("self.decided_log:",self.decided_log)
        if self.slot_to_execute == skip_slot:
            self.slot_to_execute += 1
        # while self.slot_to_execute in self.decided_log:
        while self.slot_to_execute <= max(self.decided_log.keys()):
            # proposal_id = self.decided_log[self.slot_to_execute]
            if self.slot_to_execute in self.decided_log:
                self.executed_log[self.slot_to_execute] = self.decided_log[self.slot_to_execute]
                exe = "client_id:" + str(self.executed_log[self.slot_to_execute]["client_id"]) + ", client_request_id:" + str(self.executed_log[self.slot_to_execute]["client_request_id"]) + ", request_info:" + self.executed_log[self.slot_to_execute]["request_info"] + "\n"
                # print("learner id %s executed values: %s"%(str(self.server_id), str(self.executed_log)))
                with open(self.execute_file, 'a') as f:
                    f.write(exe)
                self.slot_to_execute += 1
            else:
                self.request_missed_decided_value(self.slot_to_execute)
                return

    def process_reply(self, msg):
        if msg["value"] == None:
            return
        self.decided_log[msg["slot"]] = msg["value"]

    def request_missed_decided_value(self, slot):
        msg = {
            "type": "MISS",
            "server_id": self.server_id,
            "slot": slot
        }
        server_list_list = list(self.server_list.items())
        for i in server_list_list:
            self.sender.send(i[1]["host"], i[1]["port"], msg)

    def reply_missed_decided_value(self, msg):
        target = msg["server_id"]
        slot = msg["slot"]
        decided_value = None
        if slot in self.decided_log:
            decided_value = self.decided_log[slot]
        msg = {
            "type": "REPLY",
            "server_id": self.server_id,
            "slot": slot,
            "value": decided_value
        }
        self.sender.send(self.server_list[target]["host"], self.server_list[target]["port"], msg)
    
        