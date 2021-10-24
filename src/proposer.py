from sender import Sender

class Proposer(object):
    def __init__(self, server_id, server_list, loss):
        self.server_id = server_id
        self.acceptor_list = server_list    #{"id": {"host": "xxx", "port": xxx}}
        self.loss = loss
        self.quorum = len(server_list) // 2 + 1
        self.sender = Sender(self.loss)
        self.have_prepared = False
        self.proposal_id = None
        self.latest_proposal_id = None
        self.count_acceptor = []    #[acceptor_id]
        self.message_promise = {}   #{previous_proposal_id:{"request_info":xxx, "client_id":xxx, "slot":xxx}}
        self.available_slot = 0
        
    def prepare(self, proposal_id):
        print("prepare")
        msg = {
            "type": "PREPARE",
            "proposal_id": proposal_id,
            "server_id": self.server_id
        }
        self.latest_proposal_id = self.proposal_id
        for k, v in self.acceptor_list.items():
            self.sender.send(v["host"], v["port"], msg)
        
    def process_promise(self, msg):
        print("process_promise")
        acceptor_id = msg["acceptor_id"]
        proposal_id = msg["proposal_id"]
        if self.proposal_id != proposal_id:
            return
        self.count_acceptor.append(acceptor_id)
        # previous_proposal_id = msg["previous_proposal_id"]
        for slot in msg["accepted_proposal_id"]:
            previous_proposal_id = msg["accepted_proposal_id"][slot]
            self.message_promise[previous_proposal_id]["request_info"] = msg["accepted_request"][slot]["request_info"]
            self.message_promise[previous_proposal_id]["client_id"] = msg["acceptor_request"][slot]["client_id"]
            self.message_promise[previous_proposal_id]["slot"] = slot

    def propose(self, value):
        print("propose")
        max = None
        for k, v in self.message_promise:
            if v["slot"] == self.available_slot:
                if max == None or k > max:
                    max = k
        if max != None:
            slot = self.available_slot
            self.available_slot += 1
            request_info = self.message_promise[max]["request_info"]
            client_id = self.message_promise[max]["client_id"]
            msg = {
                "type": "PROPOSE",
                "proposal_id": self.proposal_id,
                "server_id": self.server_id,
                "slot": slot,
                "request_info": request_info,
                "client_id": client_id
            }

        else:
            slot = self.available_slot
            self.available_slot += 1
            request_info = value["request_info"]
            client_id = value["client_id"]
            msg = {
                "type": "PROPOSE",
                "proposal_id": self.proposal_id,
                "server_id": self.server_id,
                "slot": slot,
                "request_info": request_info,
                "client_id": client_id
            }

        for k, v in self.acceptor_list.items():
            self.sender.send(v["host"], v["port"], msg)
        if max != None:
            return False    # if the argument isn't proposed, return false
        else:
            return True


    def load_proposal_pack(self, proposal_pack):
        self.message_promise = proposal_pack 


