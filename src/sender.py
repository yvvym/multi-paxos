import socket
import time
import random
import pickle

from sender import Sender

class Sender(object):
    def __init__(self, loss=0):
        self.loss = loss
    
    def send(self, host, port, msg):
        loss_flag = random.random()
        if loss_flag <= self.loss:  #message is lost
            return
        
        delay = random.random()
        if delay != 0:  #message is delayed
            time.sleep(delay)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:    #send message
            s.connect((host, port))
            s.sendall(pickle.dumps(msg))
        
    