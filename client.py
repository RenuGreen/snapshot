import socket
from thread import *
import threading
import time
import json
import Queue
import traceback, random

send_channels = {}
recv_channels = []
message_queue = Queue.Queue()
lock=threading.Lock()
message_queue_lock = threading.Lock()

class Snapshot:
    snapshot_id = 0
    balance = 1000
    states = {}
    # assuming current process is 2
    # {(1, 2): {'local_state': None, 'channels': {'1': {'is_finished': False, 'messages': []}, '3': {'is_finished': False, 'messages': []}}, 'is_finished': False}}
    process_id = None
    balance_mutex = threading.Lock()
    state_mutex = threading.Lock()

    def send_money(self, amount, receiver_id):
        Snapshot.balance_mutex.acquire()
        Snapshot.balance -= amount
        Snapshot.balance_mutex.release()
        message = {'receiver_id':receiver_id, 'sender_id': Snapshot.process_id, 'message_type': 'TRANSFER', 'amount': amount}
        self.send_message(message)

    @staticmethod
    def rcv_money(message):
        Snapshot.balance_mutex.acquire()
        Snapshot.balance += message['amount'];
        print Snapshot.balance
        Snapshot.balance_mutex.release()

    def start_snapshot(self, snapshot_identifier):
        #snapshot_identifier = (Snapshot.snapshot_id, Snapshot.process_id) in case of terminal input
        self.save_local_state(snapshot_identifier)
        self.send_marker(snapshot_identifier)

    def check_marker_status(self, message):
        pass
    # 2 cases possible
    # 1. (Snapshot_id, process_id) not present in Snapshot_status , call start_snapshot(message['snapshot_id']), mark receiver channel as finished so that msgs not saved for this channel
    # 2. (Snapshot_id, process_id) present in Snapshot_status , mark receiver channel as finished so that no further msgs saved for this channel, check if all channels finished, mark snapshot as finshed

    def send_marker(self, snapshot_identifier):
        message = {'sender_id': Snapshot.process_id, 'message_type': 'MARKER', 'snapshot_id': snapshot_identifier}
        self.send_broadcast_message(message)

    def send_message(self, message):
        global message_queue_lock, message_queue
        message_queue_lock.acquire()
        message_queue.put(message)
        message_queue_lock.release()

    def save_local_state(self, index):
        Snapshot.state_mutex.acquire()
        if index not in Snapshot.states:
            temp_arr = {}
            for i in config.keys():
                if i != Snapshot.process_id:
                    temp_arr[i] = {'is_finished': False, 'messages': []}
            Snapshot.states[index] = {'local_state': None, 'is_finished': False, 'channels': temp_arr}
        Snapshot.balance_mutex.acquire()
        Snapshot.states[index]['local_state'] = Snapshot.balance
        Snapshot.balance_mutex.release()
        Snapshot.state_mutex.release()

    def save_channel_state(self, message):
        pass
    # add to all channels for which snapshot and channel not finished

    def send_broadcast_message(self, message):
        global message_queue_lock, message_queue
        for i in config.keys():
            if i != Snapshot.process_id:
                message_copy = dict(message)
                message_copy['receiver_id'] = str(i)
                message_queue_lock.acquire()
                message_queue.put(message_copy)
                message_queue_lock.release()


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
            if not i == Snapshot.process_id and not i in send_channels.keys():
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
        if message_queue.qsize() > 0:
            try:
                message_queue_lock.acquire()
                message = message_queue.get()
                message_queue_lock.release()
                receiver = message["receiver_id"]
                send_channels[receiver].sendall(json.dumps(message))
            except:
                print traceback.print_exc()


def receive_message():
    while True:
        for socket in recv_channels:
            try:
                msg = socket.recv(4096)
                if msg:
                    msg = json.loads(msg)
                    msg_type = msg["message_type"]
                    if msg_type == "TRANSFER":
                        Snapshot.rcv_money(msg)
                    else:
                        msg["snapshot_id"] = tuple(msg["snapshot_id"])
                        Snapshot.check_marker_status(msg)
            except:
                time.sleep(1)
                continue

def make_transfer():
    while True:
        time.sleep(3)
        receiver = random.randint()
        snapshot.send_money(10, )


################################################################################


with open("config.json", "r") as configFile:
    config = json.load(configFile)

Snapshot.process_id = raw_input()

# temp stub to check transfer
snapshot = Snapshot()
if Snapshot.process_id == "1":
    snapshot.send_money(10, "2")

HOST = ''
PORT = config[Snapshot.process_id]
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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
start_new_thread(make_transfer, ())

while True:
    message = raw_input("Enter SNAPSHOT: ")
    if message == "SNAPSHOT":
        pass