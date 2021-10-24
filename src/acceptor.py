from sender import Sender

class Acceptor(object):
    def __init__(self, server_id, server_list, loss):
        self.server_id = server_id
        self.learner_list = server_list
        self.proposer_list = server_list
        self.loss = loss
        self.quorum = len(server_list) // 2 + 1
        self.sender = Sender(self.loss)

        self.promised_proposal_id = None  #{slot:xxx}
        self.accepted_proposal_id = {}  #{slot:xxx}
        self.accepted_request = {}  #{slot:{"request":xxx, "client_id":xxx}}

    def promise(self, msg):
        if self.promised_proposal_id == None or self.promised_proposal_id <= msg["proposal_id"]:
            self.promised_proposal_id = msg["proposal_id"]
        else:
            return
        promise_msg = {
            "type": "PROMISE",
            "acceptor_id": self.server_id,
            "proposal_id": self.promised_proposal_id, 
            "accepted_proposal_id": self.accepted_proposal_id, 
            "accepted_request": self.accepted_request
        }
        host = self.proposer_list[str(msg["server_id"])]["host"]
        port = self.proposer_list[str(msg["server_id"])]["port"]
        self.sender.send(host, port, msg)

    def accept(self, msg):
        if self.promised_proposal_id != None and self.promised_proposal_id > msg["proposal_id"]:
            return
        slot = msg["slot"]
        request_info = msg["request_info"]
        client_id = msg["client_id"]
        self.accepted_proposal_id[slot] = msg["proposal_id"]
        self.accepted_request[slot]["request"] = request_info
        self.accepted_request[slot]["client_id"] = client_id
        msg = {
            "type": "ACCEPT", 
            "acceptor_id": self.server_id, 
            "slot": slot, 
            "request_info": request_info, 
            "client_id": client_id,
            "proposal_id": msg["proposal_id"]
        }
        for k, v in self.learner_list.items():
            self.sender.send(v["host"], v["port"], msg)

