from sender import Sender

class Proposer(object):
    def __init__(self, server_id, server_list, loss, skip, f):
        self.server_id = server_id
        self.acceptor_list = server_list    #{"id": {"host": "xxx", "port": xxx}}
        self.loss = loss
        self.skip = skip
        self.f = f
        self.quorum = len(server_list) // 2 + 1
        self.sender = Sender(self.loss)
        self.have_prepared = False
        self.proposal_id = 0
        self.count_acceptor = []    #[acceptor_id]
        self.message_promise = {}   #{proposal_id:[{"request_info":xxx, "client_id":xxx, "client_request_id": xxx, "slot":xxx, "previous_proposal_id":xxx}]}
        self.available_slot = 0
        self.proposed_list = {} #{(client_id, client_request_id): PROPOSE msg}
        
    def prepare(self, proposal_id):
        print("prepare")
        msg = {
            "type": "PREPARE",
            "proposal_id": proposal_id,
            "server_id": self.server_id
        }
        self.proposal_id = proposal_id
        for k, v in self.acceptor_list.items():
            self.sender.send(v["host"], v["port"], msg)
        
    def process_promise(self, msg):
        print("process_promise")
        acceptor_id = msg["acceptor_id"]
        proposal_id = msg["proposal_id"]
        print("self.proposal_id:", self.proposal_id, "proposal_id:", proposal_id)
        if self.proposal_id != proposal_id:
            return
        self.count_acceptor.append(acceptor_id)
        self.count_acceptor = list(set(self.count_acceptor))
        for slot in msg["accepted_proposal_id"]:
            previous_proposal_id = msg["accepted_proposal_id"][slot]
            temp = {}
            temp["request_info"] = msg["accepted_request"][slot]["request_info"]
            temp["client_id"] = msg["accepted_request"][slot]["client_id"]
            temp["client_request_id"] = msg["accepted_request"][slot]["client_request_id"]
            temp["slot"] = slot
            temp["previous_proposal_id"] = previous_proposal_id
            if self.proposal_id not in self.message_promise:
                self.message_promise[self.proposal_id] = []
            self.message_promise[self.proposal_id].append(temp)
        print("self.message_promise:", self.message_promise)

    def get_proposals(self):
        if self.proposal_id not in self.message_promise:
            return False
        accepted_requets = self.message_promise[self.proposal_id]
        max_proposal_id = {}    #{slot: max_proposal_id}
        requests_to_propose = {}    #{slot: {"client_id": xxx, "client_request_id": xxx, "request_info": xxx}}
        for r in accepted_requets:  # "slot", "client_id", "previous_proposal_id", "request_info", "client_request_id"
            slot = r["slot"]
            request_info = r["request_info"]
            client_id = r["client_id"]
            client_request_id = r["client_request_id"]
            previous_proposal_id = r["previous_proposal_id"]
            if slot not in requests_to_propose:
                requests_to_propose[slot] = {}
            if slot not in max_proposal_id or max_proposal_id[slot] < previous_proposal_id:
                max_proposal_id[slot] = previous_proposal_id
                requests_to_propose[slot]["request_info"] = request_info
                requests_to_propose[slot]["client_id"] = client_id
                requests_to_propose[slot]["client_request_id"] = client_request_id

        proposals = {}
        if len(max_proposal_id) == 0:
            self.available_slot = 0
        else:
            self.available_slot = max(max_proposal_id.keys()) + 1

        for slot in range(0, self.available_slot):
            if slot in max_proposal_id:
                proposals[slot] = {
                    "request_info": requests_to_propose[slot]["request_info"],
                    "client_id": requests_to_propose[slot]["client_id"],
                    "client_request_id": requests_to_propose[slot]["client_request_id"]
                }
            else:
                proposals[slot] = {
                    "request_info": "NOOP",
                    "client_id": None,
                    "client_request_id": None
                }
        return proposals

    def propose(self, slot, value):
        request_info = value["request_info"]
        client_id = value["client_id"]
        client_request_id = value["client_request_id"]
        msg = {
            "type": "PROPOSE",
            "proposal_id": self.proposal_id,
            "slot": slot,
            "request_info": request_info,
            "client_id": client_id,
            "client_request_id": client_request_id
        }
        if request_info != "NOOP":
            self.proposed_list[(client_id, client_request_id)] = msg
        for k, v in self.acceptor_list.items():
            self.sender.send(v["host"], v["port"], msg)

    def backup_request(self, msg):
        value = {
                "request_info": msg["request_info"],
                "client_id": msg["client_id"],
                "client_request_id": msg["client_request_id"]
            }
        if msg["request_info"] != "NOOP":
            temp = (msg["client_id"], msg["client_request_id"])
            if temp not in self.proposed_list:
                if self.available_slot == self.skip:
                    self.available_slot += 1
                self.propose(self.available_slot, value)
                self.available_slot += 1
            else:
                previous_proposal_msg = self.proposed_list[temp]
                for k, v in self.acceptor_list.items():
                    self.sender.send(v["host"], v["port"], msg)
        else:
            self.propose(self.available_slot, value)
            self.available_slot += 1

            
