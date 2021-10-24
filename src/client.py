import socket    
import sys

timeout = 10

class Client:
    """
        Client is connects to leader, and send receive messagess.
        Param:
            client_id: an unique client_id, integer
            config: config file path
            messages: message file path(for batch mode)
            mode: 0 for batch mode, 1 for manual mode
            p: loss rate
    """
    def __init__(self, client_id, host, port, server_list, messages=None, loss_rate=0):
        self.client_id = client_id
        self.loss_rate = loss_rate
        self.server_list = server_list
        self.messages = messages
        self.timeout = timeout
        self.msg_sent = 0

        # set up receiving socket
        self.host = host
        self.port = port
        self.s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.s.bind((host, port))
        self.s.settimeout(self.timeout)
    
    def run(self):            
        request_message = ['apple', 'orange', 'banana', 'pear', 'lemon']
        for i in range(len(request_message)):
            self.send_msg(request_message[i], self.server_list, i)

    def send_msg(self, msg, sockets, request_id):
        while True:
            client_info = { 'request_id': i, 'client_id': client_id, 'client_host': client_host, 'client_port': client_port }
            msg = {'type': 'request', 'request_val': val, 'resend_idx': resend_idx, 'client_info': client_info}
            for server_id in server_list:
                host = server_list[server_id]['host']
                port = server_list[server_id]['port']
    
                # send msg to (host, port)
                sender_.send(host, port, msg)
                
                
            reply = self.waitACK(client_info['clseq'])
            if reply == REPLY:
                self.chat_history[request_id]= (msg, 'ACKED')
                self.msg_sent += 1
                break
            elif reply == VIEWCHANGE:
                wrap_msg['resend_id'] = 0
            elif reply == TIMEOUT:
                wrap_msg['resend_id'] += 1
            else:
                logging.info("something wrong while waiting ack")

    def waitACK(self, client_seq_num):
        while True:
            try:
                data = self.s.recv(4096*2)
            except socket.timeout:
                logging.debug("timeout on %s", str(client_seq_num))
                return TIMEOUT
            except BlockingIOError:
                logging.debug("ioblocking")
                # time.sleep(1)
                continue
            if not data:
                continue
            msg = json.loads(data)
            if msg['type'] == REPLY:
                clseq = (msg['clseq'][0], msg['clseq'][1])
                if client_seq_num == clseq:
                    logging.info('RCVD: %s', str(msg))
                    return REPLY
            elif msg['type'] == VIEWCHANGE:
                logging.info('RCVD: %s', str(msg))
                return VIEWCHANGE

        return None