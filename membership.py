import socket
import struct
import threading
import sys
import pdb
import pickle
import time

MULTICAST_GROUP_ADDRESS = '224.1.1.1'#socket.gethostbyname(socket.gethostname())
MULTICAST_GROUP_PORT = 10000

class Server(object):
    def __init__(self, id, group = None, current_state = "candidate"):

        # Membership
        self.group = group
        self.id = id
        self.current_state = current_state

        # Comms
        self.message_id_counter = 1
        self.received_messages = {} 
        self.ordered_message_bag = []


        # sequencer parameters
        self.ordered_message_bag_flags = []
        self.expected_sequence_counter = 1

        #threading parameters
        self.mutex = threading.Lock()

        #multicast socket
        self.multicast_group = (MULTICAST_GROUP_ADDRESS, MULTICAST_GROUP_PORT)
        self.mreq = struct.pack('4sL', socket.inet_aton(self.multicast_group[0]), socket.INADDR_ANY)
        self.multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.multicast_socket.settimeout(2.0)
        self.multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.multicast_socket.bind(self.multicast_group)
        self.multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, self.mreq)
        
        self.modify_membership(group)

    def set_current_state(self):
        if self.group == None:
            self.current_state = "candidate"
        elif self.id == self.group.leader:
            self.current_state = "leader"
        elif self.id in self.group.followers:
            self.current_state = "follower"

    def modify_membership(self, membership):
        self.group = membership
        before_state = self.current_state
        self.set_current_state()
        if self.current_state == "candidate":
            self.multicast_socket.setsockopt(socket.SOL_IP, socket.IP_DROP_MEMBERSHIP, self.mreq)
    

    def report_group_membership(self):
        print("Server {0}'s view of group memembership:".format(self.id))
        print(">> socket {0}".format(self.multicast_socket.getsockname()))
        if self.group:
            membership = repr(self.group)
            return membership
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
                if self.current_state != "candidate":     
                    if isinstance(message, SequencerMessage):
                        self.ordered_message_bag_flags.append((message.sender_id, message.orig_sender, message.orig_message_id))
                    else:
                        if message.sender_id != self.id:
                            flagged_message = SequencerMessage(self.group.id, self.id, self.message_id_counter, message.sender_id, message.message_id)
                            self.multicast(flagged_message)
                        if (message.sender_id, message.message_id) not in self.received_messages: 
                            self.received_messages[(message.sender_id, message.message_id)] = False
                            with self.mutex:
                                self.ordered_message_bag.append(message)
                    with self.mutex:
                        self.update_ordered_message_bag()
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
                for follower in self.group.followers:
                    if (follower, sender_id, self.expected_sequence_counter) not in self.ordered_message_bag_flags:
                        is_delivered = False

                if is_delivered:
                    self.deliver(message)
                else:
                    new_ordered_message_bag.append(message)
        self.ordered_message_bag = new_ordered_message_bag
            
    def deliver(self, message):
        print("SUCCESS Delivering a message {0} from {1} with data {2}".format(message.message_id, message.sender_id, message.data))
        self.expected_sequence_counter += 1        
        self.received_messages[(message.sender_id, message.message_id)] = True
        return message

    def resend_ack_thread(self):
        """ Thread that re-sends all unacknowledged messages. """
        while True:
            time.sleep(2)

            with self.mutex:
                for message in self.ordered_message_bag:
                    # self.received_messages[(self.id, message.message_id)] = False
                    self.multicast(message)

    def user_input_thread(self):
        """ Thread that takes input from user and multicasts it to other processes. """
        print("Enter commands in the form of 'data' that will be packaged into messages (See membership.py line 190 for more info):")
        
        for line in sys.stdin:
            line = line[:-1]
            message = Message(group_id= self.group.id, sender_id= self.id, message_id= self.message_id_counter, data=line)
            self.multicast(message)
            self.message_id_counter += 1

    def run(self):
        print(self.report_group_membership())

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

        for thread in threads:
            thread.join()

class Group(object):
    def __init__(self, id, membership):
        self.id = id
        self.leader = membership["leader"]
        self.followers = list(membership["followers"])

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
    def __init__(self, group_id, sender_id, message_id, membershipDict, data=None):
        super(MembershipMessage, self).__init__(group_id, sender_id, message_id, data)
        self.membershipDict = membershipDict

    def repr(self):
        return super(MembershipMessage, self).repr() + 'Received Membership: {}'.format(self.membershipDict)

class SequencerMessage(Message):
    def __init__(self, group_id, sender_id, message_id, orig_sender, orig_message_id):
        self.group_id = group_id
        self.sender_id = sender_id
        self.message_id = message_id
        self.orig_sender = orig_sender
        self.orig_message_id = orig_message_id

    def repr(self):
        return 'Sequencer Message {} acknowledges "{} from {}".'.format(self.sender_id, self.orig_message_id, self.orig_sender)


class AcknowledgementMessage(Message):
    """docstring for AcknowledgementMessage"""
    def __init__(self, group_id, sender_id, message_id, data=None):
        super(AcknowledgementMessage, self).__init__(group_id, sender_id, message_id, data)

    def repr(self):
        return 'Sender {} acknowledges "{}".'.format(self.sender_id, self.data)
