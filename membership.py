import socket
import struct
import threading
import sys
import pdb
import pickle
import time
import random
from log import *
from colorama import init, Fore, Back, Style

MULTICAST_GROUP_ADDRESS = '224.1.1.1'
MULTICAST_GROUP_PORT = 10000

CLIENT_GROUP_ADDRESS = '224.1.1.2'
CLIENT_GROUP_PORT = 10000

class Server(object):
    def __init__(self, id, group_id, leader= -1, followers = set(), expected_sequence_counter = 1, current_state = "client", restore = False):

        init(autoreset=True)

        # Basic
        self.id = id
        self.current_state = current_state
        self.group = None
        self.term = 0

        # Log and DB
        self.log = Log(self.id)
        self.checkpoint = CheckPoint(self.id, self.log)
        self.in_memory_db_conn = sqlite3.connect(":memory:", check_same_thread=False)
        if restore:
            self.checkpoint.restore(self.log, self.in_memory_db_conn)
            group_id, leader, followers, expected_sequence_counter, self.term = self.checkpoint.fetch_latest_state(self.in_memory_db_conn)
        self.in_memory_db_conn.execute("CREATE TABLE IF NOT EXISTS group_membership (id INTEGER PRIMARY KEY AUTOINCREMENT, group_id, leader, followers, expected_sequence_counter, term);")
        self.log.purge()
        
        # Comms
        self.received_messages = {}
        self.log_entries = {} 
        self.ordered_message_bag = []
        self.failing_nodes = set()

        # sequencer parameters
        self.ordered_message_bag_flags = []
        self.expected_sequence_counter = expected_sequence_counter
        self.message_id_counter = expected_sequence_counter

        self.client_message_id_counter = 0

        #threading parameters
        self.mutex = threading.Condition()
        self.timeout = threading.Event()

        #multicast socket
        self.multicast_group = (MULTICAST_GROUP_ADDRESS, MULTICAST_GROUP_PORT)
        self.mreq = struct.pack('4sL', socket.inet_aton(self.multicast_group[0]), socket.INADDR_ANY)
        self.multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.multicast_socket.settimeout(2.0)
        self.multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.multicast_socket.bind(self.multicast_group)
        self.multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, self.mreq)
        
        self.client_group = (CLIENT_GROUP_ADDRESS, CLIENT_GROUP_PORT)
        self.creq = struct.pack('4sL', socket.inet_aton(self.client_group[0]), socket.INADDR_ANY)
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.client_socket.settimeout(0.5)
        self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.client_socket.bind(self.client_group)
        self.client_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, self.creq)
        
        # Membership
        self.modify_membership(group_id, leader, set(followers))

        # RAFT params
        self.yes_votes = set()
        self.no_votes = set()

    def exit(self):
        print("Saving last membership")
        self.store_state()
        self.in_memory_db_conn.close()

    def set_current_state(self):
        if self.id == self.group.leader:
            self.current_state = "leader"
        else:
            self.current_state = "follower"
        self.store_state()

    def store_state(self):
        if self.group:
            self.in_memory_db_conn.execute("INSERT INTO group_membership(group_id, leader, followers, expected_sequence_counter, term) VALUES ('{}', '{}', '{}', '{}', '{}')".format(self.group.id, self.group.leader, self.group.followers, self.expected_sequence_counter, self.term))
            self.in_memory_db_conn.commit()
            self.checkpoint.take_snapshot(self.log, self.in_memory_db_conn)
            
    def modify_membership(self, group_id, leader = -1, followers = set()):
        if group_id:    
            self.group = Group(group_id, leader, followers)
            self.set_current_state()      
        print(self.report_group_membership())
    

    def report_group_membership(self):
        #print(">> group socket {0}".format(self.multicast_socket.getsockname()))
        #print(">> client socket {0}".format(self.client_socket.getsockname()))
        if self.group:
            membership = repr(self.group)
            return "Server {0}'s view of group memembership: {1}".format(self.id, membership)
        else:
            return
    
    def multicast(self, message):
        #pdb.set_trace()
        data_string = pickle.dumps(message)
        sent = self.multicast_socket.sendto(data_string, self.multicast_group)
        self.message_id_counter += 1

    def client_multicast(self, message):
        data_string = pickle.dumps(message)
        sent = self.client_socket.sendto(data_string, self.client_group)
        self.client_message_id_counter += 1

    def receive_client_thread(self):
        """Listens for multicasted messages"""
        while True:
            if self.current_state == "leader":
                try:
                    byte_message, server = self.client_socket.recvfrom(4096)
                    client_message = pickle.loads(byte_message)
                    if not isinstance(client_message, SequencerMessage):
                        #print("Received message from client with data: {}".format(client_message.data))
                        ack = SequencerMessage(self.group.id, self.id, self.expected_sequence_counter, client_message.sender_id, client_message.message_id)
                        self.client_multicast(ack)
                        with self.mutex:
                            self.timeout.set()
                            self.mutex.wait_for(self.inbox_empty_predicate)
                            message = AppendEntryMessage(group_id = self.group.id, sender_id= self.id, message_id= self.message_id_counter, term=self.term, index = client_message.message_id, data=client_message.data)
                            self.multicast(message)
                except socket.timeout as timeout:
                    message = AppendEntryMessage(group_id = self.group.id, sender_id= self.id, message_id= self.message_id_counter, term=self.term, index = -1, data=None)
                    self.multicast(message)
                    pass
                        
                self.update_ordered_message_bag()


    def receive_thread(self):
        """Listens for multicasted messages"""
        while True:
            try:
                byte_message, server = self.multicast_socket.recvfrom(4096)
                message = pickle.loads(byte_message)
                with self.mutex:
                    #print("Received message: {}".format(message.data))
                    if message.sender_id == self.group.leader:
                        self.timeout.set()
                    if isinstance(message, SequencerMessage):
                        if message.sender_id != self.id and message.message_id == 1 and message.message_id == self.expected_sequence_counter:
                            #print("Init message received from {}".format(message.sender_id))
                            self.group.join_group(message.sender_id)
                        #else:
                        self.ordered_message_bag_flags.append((message.sender_id, message.orig_sender, message.orig_message_id))
                        if self.current_state == "leader" and message.sender_id not in ({self.group.leader} | self.group.followers):
                            print("New follower added")
                            self.group.join_group(message.sender_id)
                            leader_message = MembershipMessage(self.group.id, self.id, self.message_id_counter, self.group.id, self.group.leader, self.group.followers, self.term)
                            self.multicast(leader_message)
                    elif isinstance(message, ElectionMessage):
                        #print("{} {}".format(message.term, self.term))
                        self.timeout.set()
                        if message.term > self.term:
                            message.vote = message.term > self.term
                            self.term = message.term
                            message.sender_id = self.id
                            self.yes_votes = set()
                            self.no_votes = set()
                            self.current_state = "follower"
                            self.expected_sequence_counter = message.message_id + 1
                            self.message_id_counter = message.message_id + 1
                            self.multicast(message)
                        elif message.orig_sender == self.id and message.term == self.term:
                            self.expected_sequence_counter = message.message_id + 1
                            self.message_id_counter = message.message_id + 1
                            # Count and decide
                            if message.vote:
                                self.yes_votes.add(message.sender_id)
                            else:
                                self.no_votes.add(message.sender_id)
                            if self.current_state == "candidate":
                                if len(self.yes_votes) >= (self.group.group_size()//2 + 1):
                                    #print("Election Success: yes -> {}, no -> {}".format(self.yes_votes, self.no_votes))
                                    self.yes_votes.remove(self.id)
                                    self.modify_membership(self.group.id, self.id, set(self.yes_votes | self.no_votes))
                                    leader_message = MembershipMessage(self.group.id, self.id, self.message_id_counter, self.group.id, self.group.leader, self.group.followers, self.term)
                                    self.yes_votes = set()
                                    self.no_votes = set()
                                    self.multicast(leader_message)
                                elif len(self.no_votes) >= (self.group.group_size()//2 + 1):
                                    #print("Stepping down")
                                    self.term = message.term
                                    self.yes_votes = set()
                                    self.no_votes = set()
                                    self.current_state = "follower"
                        elif message.term < self.term:
                            message.vote = message.term > self.term
                            message.term = self.term
                            message.sender_id = self.id
                            self.expected_sequence_counter = message.message_id + 1
                            self.message_id_counter = message.message_id + 1
                            self.multicast(message)
                            if self.current_state == "leader":
                                leader_message = MembershipMessage(self.group.id, self.id, self.message_id_counter, self.group.id, self.group.leader, self.group.followers, self.term)
                                self.multicast(leader_message)
                    else:
                        flagged_message = SequencerMessage(self.group.id, self.id, self.expected_sequence_counter, message.sender_id, message.message_id)
                        self.multicast(flagged_message)
                        if message.message_id not in self.received_messages: 
                            self.received_messages[message.message_id] = message.data
                            self.ordered_message_bag.append(message)
                            if message.data:
                                print(Fore.RED + message.data)
                        pass
                    self.update_ordered_message_bag()
                    self.mutex.notify()
                continue
            except socket.timeout:
                pass

    def update_ordered_message_bag(self):
        for message in self.ordered_message_bag: 
            message_id = message.message_id
            sender_id = message.sender_id
            is_delivered = True
            members = self.group.followers.copy()
            self.failing_nodes = set()
            if len(members):
                for member in members:
                    if (member, sender_id, self.expected_sequence_counter) not in self.ordered_message_bag_flags:
                        is_delivered = False
                        self.failing_nodes.add(member)
                        #print((member, sender_id, self.expected_sequence_counter))
                        pass
                #Uncomment if node failure detection not needed
                if len(self.failing_nodes) <= len(self.group.followers)/2:
                    is_delivered = True
            #else:
            #    is_delivered = False
            
            if self.current_state != "leader" or is_delivered:
                self.handle_success(message)              
    
    def replace_old_message(self, old, new):
        if self.is_same_message(old, new):
            return new
        else:
            return old

    def is_same_message(self, old, new):
        return old.message_id == new.message_id and old.sender_id == new.sender_id

    def handle_success(self, message):
        """
        Flow reaches here only when all nodes receive this message. 
        Also, listeners receive this because they need to hear membership updates
        """
        #if message.data != None or message.data != 'heartbeat':
        #print("Success delivering a message {0} from {1} with data '{2}'".format(int(message.message_id), message.sender_id, message.data))
        #if message.data != None and message.data != 'heartbeat':
        
        self.ordered_message_bag = [ x for x in self.ordered_message_bag if not self.is_same_message(x, message) ]
        self.received_messages[(message.sender_id, message.message_id)] = True
        self.expected_sequence_counter = message.message_id + 1
        self.message_id_counter = message.message_id + 1
        self.failing_nodes = set()
        if isinstance(message, MembershipMessage):
            self.modify_membership(message.new_group_id, message.new_leader, message.new_followers)
            self.term = message.term
        elif isinstance(message, AppendEntryMessage):
            if self.received_messages.get(message.message_id - 1):
                print(Fore.GREEN + self.received_messages.get(message.message_id - 1))
        return message

    def resend_ack_thread(self):
        """ Thread that re-sends all unacknowledged messages. """
        while True:
            time.sleep(0.5)
            with self.mutex:
                if len(self.ordered_message_bag) != 0:
                    for message in self.ordered_message_bag:
                        if message.ttl == 0:
                            print("Non responding nodes {}".format(self.failing_nodes))
                            self.handle_failure(message)
                        elif message.sender_id == self.id:
                            message.ttl -= 1
                self.mutex.notify()
    
    def handle_failure(self, message):
        # Reset stuff
        self.ordered_message_bag = [ x for x in self.ordered_message_bag if not self.is_same_message(x, message) ]
        self.received_messages.pop(message.message_id)
                        
        # Resend with failed messages
        if self.current_state == "leader":
            # Update membership
            for failing_node in self.failing_nodes:
                self.group.leave_group(failing_node)
            leader_message = MembershipMessage(self.group.id, self.id, self.message_id_counter, self.group.id, self.group.leader, self.group.followers, self.term)
            self.multicast(leader_message)
            #print(self.report_group_membership())
            #if message.sender_id == self.id:
            message.ttl = 16
            self.ordered_message_bag.append(message)

    def user_input_thread(self):
        """ Thread that takes input from user and multicasts it to other processes. """
        print("Enter client commands in the form of 'data' that will be packaged into messages:")
        while True:
            line = input(">> ")
            message = Message(group_id= None, sender_id= self.id, message_id = self.message_id_counter, data=line)
            self.client_multicast(message)
            while True:
                try:
                    byte_message, server = self.client_socket.recvfrom(4096)
                    message = pickle.loads(byte_message)
                    if isinstance(message, SequencerMessage) and message.orig_sender == self.id :
                        #print(" << Received acknowledgement for order {}".format(message.orig_message_id))
                        break
                except socket.timeout:
                    print(" << Timed out waiting for acknowledgement, resending...")
                    #self.client_multicast(message)
                    pass
    '''
    TODO: output thread for clients to listen

    def user_output_thread(self):
        """ Thread that takes input from user and multicasts it to other processes. """
        while True:
            try:
                byte_message, server = self.client_socket.recvfrom(4096)
                message = pickle.loads(byte_message)
                if isinstance(message, SequencerMessage) and message.orig_sender != self.id :
                    #print(" << Received acknowledgement for order {}".format(message.orig_message_id))
                    pass
                else:
                    if 
                    print(">> {}")
            except socket.timeout:
                print(" << Timed out waiting for acknowledgement, resending...")
                #self.client_multicast(message)
    '''
    def inbox_empty_predicate(self):
        self.timeout.set()
        return len(self.ordered_message_bag) == 0

    def timer_thread(self):
        init_message = SequencerMessage(self.group.id, self.id, self.expected_sequence_counter, self.id, 0)
        self.multicast(init_message)
        time.sleep(1)
        while True:
            timeDelay = random.randrange(1000, 1500, 1)/1000
            while self.timeout.wait(timeDelay):
                # Message arrived / flag set
                self.timeout.clear()

            # Handle timeout/ start election
            print("Timed out after {} s".format(timeDelay))
            self.current_state = "candidate"
            self.yes_votes = set()
            self.yes_votes.add(self.id)
            self.no_votes = set()
            self.timeout.set()
            self.term += 1
            request_vote = ElectionMessage(self.group.id, self.id, self.message_id_counter, self.id, self.message_id_counter, self.term, True)
            self.multicast(request_vote)

    def run(self):
        #Start the server by initializing values and starting threads
        if self.current_state == "client":
            thread_functions = [
                self.user_input_thread
            ]
        else:
            thread_functions = [
                self.resend_ack_thread,
                self.receive_thread,
                self.timer_thread,
                self.receive_client_thread
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
            self.followers.add(server)

    def leave_group(self, server):
        if server in self.followers:
            self.followers.remove(server)

    def group_size(self):
        return len(self.followers) + 1

    def __repr__(self):
        return '{' + '"leader": {0}, "followers": {1}'.format(str(self.leader), str(self.followers)) + '}'

class Message(object):
    """ Try to make subclass for specific messages """
    def __init__(self, group_id, sender_id, message_id, data=None):
        self.group_id = group_id
        self.sender_id = sender_id
        self.message_id = message_id
        self.data = data
        self.ttl = 32

    def repr(self):
        return "Message: sequence {} for group {} from {} with data '{}'.".format(int(self.message_id),
            self.group_id, int(self.sender_id), self.data)

class MembershipMessage(Message):
    def __init__(self, group_id, sender_id, message_id, new_group_id, new_leader, new_followers, term, data=None):
        super(MembershipMessage, self).__init__(group_id, sender_id, message_id, data)
        self.new_group_id = new_group_id
        self.new_leader = new_leader
        self.new_followers = new_followers
        self.term = term
    def repr(self):
        return super(MembershipMessage, self).repr() + 'New Membership: {}'.format(Group(self.new_group_id, self.new_leader, self.new_followers).__repr__())

class SequencerMessage(Message):
    def __init__(self, group_id, sender_id, message_id, orig_sender, orig_message_id):
        super(SequencerMessage, self).__init__(group_id, sender_id, message_id)
        self.orig_sender = orig_sender
        self.orig_message_id = orig_message_id
    def repr(self):
        return super(SequencerMessage, self).repr() + 'Sequencer Message: {} acknowledges "{} from {}".'.format(self.sender_id, self.orig_message_id, self.orig_sender)

class ElectionMessage(Message):
    def __init__(self, group_id, sender_id, message_id, orig_sender, orig_message_id, term, vote):
        super(ElectionMessage, self).__init__(group_id, sender_id, message_id)
        self.orig_sender = orig_sender
        self.orig_message_id = orig_message_id
        self.term = term
        self.vote = vote

    def repr(self):
        return super(ElectionMessage, self).repr() + 'Election Message: {} votes {} for {} in term {}.'.format(self.sender_id, self.vote, self.orig_sender, self.term)

class AppendEntryMessage(Message):
    def __init__(self, group_id, sender_id, message_id, term, index, data=None):
        super(AppendEntryMessage, self).__init__(group_id, sender_id, message_id, data)
        self.term = term
        self.index = index

    def repr(self):
        return super(AppendEntryMessage, self).repr() + ' AppendEntryMessage: Commit entry at {}, add entry {} with data "{}".'.format(self.message_id - 1, self.message_id, self.data, self.term)