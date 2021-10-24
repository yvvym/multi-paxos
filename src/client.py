import socket    
import sys
import logging
from utils import *

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    stream=sys.stdout
                    )

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
    def __init__(self, client_id, host, port, replica_list, messages=None, loss_rate=0):
        self.client_id = client_id
        self.loss_rate = loss_rate
        self.replica_list = replica_list
        self.messages = messages
        self.chat_history = {}
        self.timeout = PROCESS_TIMEOUT
        self.msg_sent = 0
        # self.lock = threading.Lock()

        # set up receiving socket
        self.host = host
        self.port = port
        self.s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.s.bind((host, port))
        self.s.settimeout(self.timeout)
    
    def run(self):            
        self.run_batch_mode()

    def run_batch_mode(self):
        with open(self.messages, 'r') as f:
            msgs = f.readlines()
        for i in range(len(msgs)):
            self.send_msg(msgs[i].rstrip(), self.replica_list, i)

    def send_msg(self, msg, sockets, request_id):
        client_info = {
            'clseq': (self.client_id, request_id),
            'port': self.port,
            'host': self.host
        }
        wrap_msg = {
            'type': REQUEST,
            'value': msg,
            'client_info': client_info,
            'resend_id': 0
            }
        self.chat_history[request_id] = (msg, 'NACK')
        while True:
            for replica in sockets:
                send(replica.host, replica.port, wrap_msg, self.loss_rate)
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