import socket
import time
import random
import pickle


class Sender(object):
    def __init__(self, loss=0):
        self.loss = loss
    
    def send(self, host, port, msg):
        loss_flag = random.random()
        if loss_flag <= self.loss:  #message loss
            return "losss"
        
        delay = random.random()
        if delay != 0:  #message is delayed
            time.sleep(delay)
        
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((host, port))
        except socket.error:
            print("socket.error")
            return

        try:
            s.sendall(pickle.dumps(msg))
        except socket.error:
            print("socket.error, resend")
            time.sleep(0.1)
            s.sendall(pickle.dumps(msg))

    