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
        self.message_id_counter = 0
        self.received_messages = {} # a dictionary for sender_id: sequence_number pairs to keep track of what messages this server has received
        self.acknowledged_messages = {}
        self.unacknowledged_messages = []
        self.ordered_message_bag = []


        # sequencer parameters
        self.ordered_message_bag_flags = []
        self.expected_sequence_counter = 0

        #params that are only directly used by sequencer node
        self.sequence_counter = 0
        self.sequencer_id = 0

        #threading parameters
        self.mutex = threading.Lock()


        self.multicast_group = (MULTICAST_GROUP_ADDRESS, MULTICAST_GROUP_PORT)
        #multicast socket
        self.multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.multicast_socket.settimeout(2.0)
        #self.multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, struct.pack('b', 1))

        #listen socket
        self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listen_socket.bind(('', self.multicast_group[1]))
        self.listen_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, struct.pack('4sL', socket.inet_aton(self.multicast_group[0]), socket.INADDR_ANY))
        self.listen_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, struct.pack('b', 1))


        self.modify_membership(group)
        #self.handle_connection()

    def set_current_state(self):
        if self.group == None:
            self.current_state = "candidate"
        elif self.id == self.group.leader:
            self.current_state = "leader"
        elif self.id in self.group.followers:
            self.current_state = "follower"

    """
        def handle_connection(self):
            socket bind for follower
            self.socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, struct.pack('b', 1))
            if self.current_state != "leader":
                self.socket.bind(self.multicast_group)
    """

    def modify_membership(self, membership):
        self.group = membership
        before_state = self.current_state
        self.set_current_state()

        """
        if before_state != self.current_state:
            mreq = struct.pack('4sL', socket.inet_aton(self.multicast_group[0]), socket.INADDR_ANY)
            if self.current_state == "follower":
                self.socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            elif self.current_state == "candidate":
                self.socket.setsockopt(socket.SOL_IP, socket.IP_DROP_MEMBERSHIP, mreq)
        """

    def report_group_membership(self):
        """
        >>> membership = {"leader": 0, "followers": []}
        >>> leader = Server(0, Group(membership))
        >>> leader.report_group_membership()
        Server 0's view of group memembership:
        '{"leader": 0, "followers": []}'

        >>> membership = {"leader": 2, "followers": [1, 3, 5]}
        >>> follwer = Server(3, membership)
        >>> follwer.report_group_membership()
        Server 3's view of group memembership:
        "{'leader': 2, 'followers': [1, 3, 5]}"
        """
        print("Server {0}'s view of group memembership:".format(self.id))
        print(">> socket {0}".format(self.multicast_socket.getsockname()))
        if self.group:
            membership = repr(self.group)
            return membership
        else:
            return

    def unicast(self, message, server):
        """"""
        data_string = pickle.dumps(message)
        sent = self.multicast_socket.sendto(data_string, server)
        return sent

    def multicast(self, message):
        #pdb.set_trace()
        for follower in self.group.followers:
            with self.mutex:
                self.unacknowledged_messages.append((follower, message))
        data_string = pickle.dumps(message)
        #sent = self.unicast(data_string, self.socket.getsockname()) #send message to self also
        sent = self.multicast_socket.sendto(data_string, self.multicast_group)
        self.message_id_counter += 1

    def receive_thread(self):
        """Listens for multicasted messages"""

        while True:
            try:
                byte_message, server = self.listen_socket.recvfrom(4096)
                print("Received message")
                message = pickle.loads(byte_message)
                if isinstance(message, AcknowledgementMessage):
                    self.acknowledged_messages[(message.sender_id, message.message_id)] = True
                else:
                    ack = AcknowledgementMessage(self.group.id, self.id, message.message_id, data=None)
                    print(type(server))
                    print(server)
                    self.unicast(ack, server)#send acknowledgment to sender
                    if (message.sender_id, message.message_id) not in self.received_messages:
                        self.received_messages[(message.sender_id, message.message_id)] = True
                        if isinstance(message, SequencerMessage):
                            self.ordered_message_bag_flags.append((message.sequence_counter, message.orig_sender, message.orig_message_id))
                            self.update_ordered_message_bag()
                        else:
                            if self.id == self.sequencer_id:
                                #(self, group_id, sender_id, message_id, data, sequence_counter, orig_sender, orig_message_id)
                                flagged_message = SequencerMessage(self.group.id, self.id, self.message_id_counter, self.sequence_counter, message.sender_id, message.message_id)
                                self.multicast(flagged_message)
                                self.sequence_counter += 1
                            self.ordered_message_bag.append((message.sender_id, message.message_id, message))
                            self.update_ordered_message_bag()
                continue
            except socket.timeout:
                pass

    def update_ordered_message_bag(self):
        while True:
            new_ordered_message_bag = []
            is_ever_delivered = False
            for sender, message_id, message in self.ordered_message_bag:
                is_delivered = False
                for sequence_counter, orig_sender, orig_message_id in self.ordered_message_bag_flags:
                    if sender == orig_sender and message_id == orig_message_id and self.expected_sequence_counter == sequence_counter:
                        self.deliver(sender, message)
                        is_delivered = True
                        is_ever_delivered = True
                        self.expected_sequence_counter += 1
                        break

                if not is_delivered:
                    new_ordered_message_bag.append((sender, message_id, message))

            self.ordered_message_bag = new_ordered_message_bag

            if not is_ever_delivered:
                break

    def deliver(self, sender, message):
        print("SUCCESSSS Delivering a message from: {}".format(sender))
        return message

    def resend_ack_thread(self):
        """ Thread that re-sends all unacknowledged messages. """
        while True:
            time.sleep(0.2)

            with self.mutex:
                temp_unack_messages = []
                for dest_id, message in self.unacknowledged_messages:
                    if (dest_id, message.message_id) not in self.acknowledged_messages:
                        temp_unack_messages.append((dest_id, message))
                        self.unicast(message)
                self.unacknowledged_messages = temp_unack_messages

    def user_input_thread(self):
        """ Thread that takes input from user and multicasts it to other processes. """
        print("Enter commands in the form of 'data' that will be packaged into messages (See membership.py line 190 for more info):")
        for line in sys.stdin:
            line = line[:-1]
            message = Message(group_id= self.group.id, sender_id= self.id, message_id= self.message_id_counter, data=line)
            self.multicast(message)

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

        for thread in threads:
            thread.join()

class Group(object):
    """
    >>> membership = {"leader": 0, "followers": [1, 2, 3, 4, 5]}
    >>> first_group = Group(membership)
    >>> first_group
    {"leader": 0, "followers": [1, 2, 3, 4, 5]}
    >>> first_group.join_group(8)
    >>> first_group
    {"leader": 0, "followers": [1, 2, 3, 4, 5, 8]}
    >>> first_group.leave_group(3)
    >>> first_group
    {"leader": 0, "followers": [1, 2, 4, 5, 8]}
    """
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
    def __init__(self, group_id, sender_id, message_id, sequence_counter, orig_sender, orig_message_id):
        self.group_id = group_id
        self.sender_id = sender_id
        self.message_id = message_id
        self.sequence_counter = sequence_counter
        self.orig_sender = orig_sender
        self.orig_message_id = orig_message_id

class AcknowledgementMessage(Message):
    """docstring for AcknowledgementMessage"""
    def __init__(self, group_id, sender_id, message_id, data=None):
        super(AcknowledgementMessage, self).__init__(group_id, sender_id, message_id, data)

    def repr(self):
        return 'Sender {} acknowledges "{}".'.format(self.sender_id, self.data)
