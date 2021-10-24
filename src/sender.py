import socket
import time
import random
import pickle


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

        # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:    #send message
        #     s.connect((host, port))
        #     s.sendall(pickle.dumps(msg))
        
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((host, port))
        except socket.error:
            print("listening port closed, ignore this msg")
            return

        try:
            s.sendall(pickle.dumps(msg))
        except socket.error:
            print("try to resend due to socket error")
            time.sleep(0.1)
            s.sendall(pickle.dumps(msg))

    