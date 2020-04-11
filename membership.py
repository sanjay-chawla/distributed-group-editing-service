import socket
import struct
import threading
import sys
import pdb
import pickle
import time
from log import *

MULTICAST_GROUP_ADDRESS = '224.1.1.1'#socket.gethostbyname(socket.gethostname())
MULTICAST_GROUP_PORT = 10000

class Server(object):
    def __init__(self, id, group_id, leader, followers, expected_sequence_counter = 1, current_state = "candidate", restore = True):

        # Basic
        self.id = id
        self.current_state = current_state

        # Log and DB
        self.log = Log(self.id)
        self.log.purge()
        self.checkpoint = CheckPoint(self.id, self.log)
        self.in_memory_db_conn = sqlite3.connect(":memory:", check_same_thread=False)
        if restore:
            self.checkpoint.restore(self.log, self.in_memory_db_conn)
            group_id, leader, followers, expected_sequence_counter = self.checkpoint.fetch_latest_state(self.in_memory_db_conn)
        self.in_memory_db_conn.execute("CREATE TABLE IF NOT EXISTS group_membership (id INTEGER PRIMARY KEY AUTOINCREMENT, group_id, leader, followers, expected_sequence_counter);")
        
        # Comms
        self.message_id_counter = expected_sequence_counter
        self.received_messages = {} 
        self.ordered_message_bag = []

        # sequencer parameters
        self.ordered_message_bag_flags = []
        self.expected_sequence_counter = expected_sequence_counter

        #threading parameters
        self.mutex = threading.Condition()

        #multicast socket
        self.multicast_group = (MULTICAST_GROUP_ADDRESS, MULTICAST_GROUP_PORT)
        self.mreq = struct.pack('4sL', socket.inet_aton(self.multicast_group[0]), socket.INADDR_ANY)
        self.multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.multicast_socket.settimeout(2.0)
        self.multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.multicast_socket.bind(self.multicast_group)
        self.multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, self.mreq)
        
        # Membership
        self.modify_membership(group_id, leader, followers)

    def exit(self):
        print("Saving last membership")
        self.store_state()
        self.in_memory_db_conn.close()

    def set_current_state(self):
        if self.id == self.group.leader:
            self.current_state = "leader"
        elif self.id in self.group.followers:
            self.current_state = "follower"
        else:
            self.current_state = "candidate"
        self.store_state()

    def store_state(self):
        self.in_memory_db_conn.execute("INSERT INTO group_membership(group_id, leader, followers, expected_sequence_counter) VALUES ('{}', '{}', '{}', '{}')".format(self.group.id, self.group.leader, self.group.followers, self.expected_sequence_counter))
        self.in_memory_db_conn.commit()
        self.checkpoint.take_snapshot(self.log, self.in_memory_db_conn)
        
    def modify_membership(self, group_id, leader, followers):
        self.group = Group(group_id, leader, followers)
        before_state = self.current_state
        self.set_current_state()    
        
        print(self.report_group_membership())
    

    def report_group_membership(self):
        # print(">> socket {0}".format(self.multicast_socket.getsockname()))
        if self.group:
            membership = repr(self.group)
            return "Server {0}'s view of group memembership: {1}".format(self.id, membership)
        else:
            return
    
    def multicast(self, message):
        #pdb.set_trace()
        data_string = pickle.dumps(message)
        sent = self.multicast_socket.sendto(data_string, self.multicast_group)

    def receive_thread(self):
        """Listens for multicasted messages"""
        while True:
            try:
                byte_message, server = self.multicast_socket.recvfrom(4096)
                message = pickle.loads(byte_message)
                # print("Received message: {}".format(message.repr())) 
                if isinstance(message, SequencerMessage):
                    self.ordered_message_bag_flags.append((message.sender_id, message.orig_sender, message.orig_message_id))
                else:
                    flagged_message = SequencerMessage(self.group.id, self.id, self.message_id_counter, message.sender_id, message.message_id)
                    self.multicast(flagged_message)
                    if (message.sender_id, message.message_id) not in self.received_messages: 
                        self.received_messages[(message.sender_id, message.message_id)] = False
                        with self.mutex:
                            self.ordered_message_bag.append(message)
                with self.mutex:
                    self.update_ordered_message_bag()
                    self.mutex.notify()
                continue
            except socket.timeout:
                pass

    def update_ordered_message_bag(self):
        new_ordered_message_bag = []
        for message in self.ordered_message_bag: 
            message_id = message.message_id
            sender_id = message.sender_id          
            is_delivered = True
            if self.group != None:
                listeners = self.group.followers.copy()
                listeners.append(self.group.leader)
                for listener in listeners:
                    if (listener, sender_id, self.expected_sequence_counter) not in self.ordered_message_bag_flags:
                        is_delivered = False
                        # print((listener, sender_id, self.expected_sequence_counter))
                # print(is_delivered)
                if is_delivered:
                    self.deliver(message)
                else:
                    new_ordered_message_bag.append(message)
        self.ordered_message_bag = new_ordered_message_bag
            
    def deliver(self, message):
        """
        Flow reaches here only when all nodes receive this message. 
        Also, candidates receive this because they need to hear membership updates
        """
        print("SUCCESS Delivering a message {0} from {1} with data {2}".format(message.message_id, message.sender_id, message.data))
        self.expected_sequence_counter = message.message_id + 1    
        self.message_id_counter = self.expected_sequence_counter
        self.received_messages[(message.sender_id, message.message_id)] = True
        if isinstance(message, MembershipMessage):
            self.modify_membership(message.new_group_id, message.new_leader, message.new_followers)
        return message

    def resend_ack_thread(self):
        """ Thread that re-sends all unacknowledged messages. """
        while True:
            time.sleep(0.2)

            with self.mutex:
                for message in self.ordered_message_bag:
                    # self.received_messages[(self.id, message.message_id)] = False
                    if message.sender_id == self.id:
                        self.multicast(message)

    def user_input_thread(self):
        """ Thread that takes input from user and multicasts it to other processes. """
        print("Enter commands in the form of 'data' that will be packaged into messages (See membership.py line 190 for more info):")
        
        for line in sys.stdin:
            with self.mutex:
                self.mutex.wait_for(self.inbox_empty_predicate)
                line = line[:-1]
                if line.startswith("mem"):
                    params = line.replace("mem ", "").split(" ")
                    group_id = list(map(int, params[0]))[0]
                    leader = list(map(int, params[1]))[0]
                    followers = list(map(int, params[2].replace("[","").replace("]","").split(",")))
                    message = MembershipMessage(self.group.id, self.id, self.message_id_counter, group_id, leader, followers, data=None)
                    # print(message.repr())
                else:
                    message = Message(group_id= self.group.id, sender_id= self.id, message_id= self.message_id_counter, data=line)
                self.multicast(message)
                self.message_id_counter += 1 

    def inbox_empty_predicate(self):
        return len(self.ordered_message_bag) == 0

    def run(self):
        #Start the server by initializing values and starting threads
        thread_functions = [
            self.resend_ack_thread,
            self.receive_thread,
            self.user_input_thread,
        ]

        threads = []
        for thread_function in thread_functions:
            thread = threading.Thread(target=thread_function)
            thread.daemon = True
            thread.start()
            threads.append(thread)

        try:
            for thread in threads:
                thread.join()
        except KeyboardInterrupt:
            print("Ending execution")

class Group(object):
    def __init__(self, id, leader, followers):
        self.id = id
        self.leader = leader
        self.followers = followers

    def join_group(self, server):
        if server not in self.followers:
            self.followers.append(server)

    def leave_group(self, server):
        if server in self.followers:
            self.followers.remove(server)

    def group_size(self):
        return len(self.followers) + 1

    def __repr__(self):
        return '{' + '"leader": {0}, "followers": {1}'.format(str(self.leader), str(self.followers)) + '}'

class Message(object):
    """ Try to make subclass for specific messages """
    def __init__(self, group_id=None, sender_id=None, message_id=None, data=None):
        self.group_id = group_id
        self.sender_id = sender_id
        self.message_id = message_id
        self.data = data

    def repr(self):
        return "Message sequence {} for group {} from {}.".format(self.message_id,
            self.group_id, self.sender_id, self.data)

class MembershipMessage(Message):
    def __init__(self, group_id, sender_id, message_id, new_group_id, new_leader, new_followers, data=None):
        super(MembershipMessage, self).__init__(group_id, sender_id, message_id, data)
        self.new_group_id = new_group_id
        self.new_leader = new_leader
        self.new_followers = new_followers

    def repr(self):
        return super(MembershipMessage, self).repr() + 'New Membership: {}'.format(Group(self.new_group_id, self.new_leader, self.new_followers).__repr__())

class SequencerMessage(Message):
    def __init__(self, group_id, sender_id, message_id, orig_sender, orig_message_id):
        self.group_id = group_id
        self.sender_id = sender_id
        self.message_id = message_id
        self.orig_sender = orig_sender
        self.orig_message_id = orig_message_id

    def repr(self):
        return 'Sequencer Message {} acknowledges "{} from {}".'.format(self.sender_id, self.orig_message_id, self.orig_sender)

