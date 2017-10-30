import socket
import sys
from thread import *
import threading
import time
import json
import Queue

class Snapshot:
    snapshot_id = 0
    balance = 1000
    states = {}
    balance_mutex = threading.Lock()

    def send_money(self, amount):
        Snapshot.balance_mutex.acquire()


        Snapshot.balance_mutex.release()

    def rcv_money(self, amount):
        pass

    def start_snapshot(self):
        pass

    def start_recording(self):
        pass

    def rcv_marker(self, message):
        pass

    def stop_recording(self):
        pass

    def send_message(self, message):
        pass

    def save_local_state(self, state):
        pass

    def channel_state(self, message):
        pass

    def send_broad_message(self, message):
        pass



def setup_receive_channels(s):
    while 1:
        try:
            conn, addr = s.accept()
        except:
            continue
        recv_channels.append(conn)
        print 'Connected with ' + addr[0] + ':' + str(addr[1])
        # what to do after connecting to all clients
        # should I break?

def setup_send_channels():
    while True:
        for i in config.keys():
            if not i == process_id and not i in send_channels.keys():
                cs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                host = '127.0.0.1'
                port = config[i]
                try:
                    cs.connect((host, port))
                except:
                    time.sleep(1)
                    continue
                print 'Connected to ' + host + ' on port ' + str(port)
                send_channels[i] = cs   #add channel to dictionary
        if len(send_channels.keys()) == len(config.keys()) -1:
            break

def send_message():
    while True:
        lock.acquire()
        if message_queue.qsize() > 0:
            message = message_queue.get()
            receiver = message.split(',')[0]
            try:
                send_channels[receiver].sendall(message)
            except Exception:
                import traceback
                print traceback.format_exc()
        lock.release()

def receive_message():
    while True:
        for socket in recv_channels:
            try:
                msg = socket.recv(4096)

                if msg:
                    print "Message received: " + msg
            except:
                time.sleep(1)
                continue


################################################################################


with open("config.json", "r") as configFile:
    config = json.load(configFile)

process_id = raw_input()
send_channels = {}
recv_channels = []
message_queue = Queue.Queue()
lock=threading.Lock()
HOST = ''
PORT = config[process_id]
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setblocking(0)
s.bind((HOST, PORT))
s.listen(10)

start_new_thread(setup_receive_channels, (s,))
t1 = threading.Thread(target=setup_send_channels, args=())
t1.start()
# wait till all send connections have been set up
t1.join()
start_new_thread(send_message, ())
start_new_thread(receive_message, ())

while True:
    message = raw_input("Enter message in the format RECEIVER,SENDER,MESSAGE ")
    lock.acquire()
    message_queue.put(message)
    lock.release()