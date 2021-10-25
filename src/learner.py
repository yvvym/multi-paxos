from acceptor import Acceptor
from sender import Sender
import logging

class Learner(object):
    def __init__(self, server_id, server_list, loss, client_list):
        self.server_id = server_id
        self.loss = loss
        self.client_list = client_list
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

        self.proposal_list = {} #{proposal_id:{"request_info":xxx, "client_id":xxx}}
        self.decided_log = {}   #{slot:proposal_id}
        self.executed_log = {}  #{slot:proposal_id}
        self.slots = {} #{slot:{proposal_id:[acceptors]}}
        self.slot_to_execute = 0
    
    def add_accept(self, msg):
        print("add_accept")
        acceptor_id = msg["acceptor_id"]
        slot = msg["slot"]
        request_info = msg["request_info"]
        client_id = msg["client_id"]
        proposal_id = msg["proposal_id"]
        if slot not in self.slots:
            self.slots[slot] = {}
        self.slots[slot][proposal_id] = []
        self.slots[slot][proposal_id].append(acceptor_id)
        
        if proposal_id not in self.proposal_list:
            self.proposal_list[proposal_id] = {}
            self.proposal_list[proposal_id]["request_info"] = request_info
            self.proposal_list[proposal_id]["client_id"] = client_id
            

    def majority_have_accepted(self, proposal_id, slot):
        print("majority_have_accepted")
        print(self.slots, "self.slots:", self.slots[slot])
        count = len(self.slots[slot][proposal_id])
        print(count, "++++++++count:", self.quorum)
        if count >= self.quorum:
            print("+++++++++++++True")
            return True
        else:
            print("+++++++++++++False")
            return False
        
    def decide(self, proposal_id, slot):
        print("decide")
        self.decided_log[slot] = proposal_id
        decided_request_info = self.proposal_list[proposal_id]["request_info"]
        decided_client_id = self.proposal_list[proposal_id]["client_id"]
        log = "learner " + str(self.server_id) + " decided slot "+ str(slot) + " and request " + decided_request_info + " by client " + str(decided_client_id)
        self.file_logger.info(log)
        msg = {
            "type": "ACK",
            "val": decided_request_info,
            "client_info": decided_client_id
        }
        for k, v in self.client_list.items():
            self.sender.send(v["host"], v["port"], msg)


    def execute(self):
        print("execute")
        while self.slot_to_execute in self.decided_log:
            proposal_id = self.decided_log[self.slot_to_execute]
            self.executed_log[self.slot_to_execute] = proposal_id
            exe = str(self.proposal_list[proposal_id]["client_id"]) + " " + self.proposal_list[proposal_id]["request_info"]
            print("learner id %s executed values: %s"%(str(self.server_id), str(self.executed_log)))
            with open(self.execute_file, 'a') as f:
                f.write(exe)
            self.slot_to_execute += 1


    def get_proposal_pack(self):
        proposal_pack = {}
        for k, v in self.decided_log:
            proposal_pack_tmp = {}
            proposal_pack_tmp["request_info"] = self.proposal_list[v]["request_info"]
            proposal_pack_tmp["client_id"] = self.proposal_list[v]["client_id"]
            proposal_pack_tmp["slot"] = k
        proposal_pack[v] = proposal_pack_tmp
        return proposal_pack
        