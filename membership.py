#!/usr/bin/python

import socket
import struct
import sys
import pdb
import pickle
import Queue as queue


MULTICAST_GROUP_ADDRESS = '224.1.1.1'#socket.gethostbyname(socket.gethostname())
MULTICAST_GROUP_PORT = 10000

class Server(object):
    def __init__(self, id, group = None, current_state = "candidate"):

        self.id = id

        # Membership
        self.group = group
        self.current_state = current_state

        # Comms
        self.sequence_number = 1
        self.received_messages = {} # a dictionary for sender_id: sequence_number pairs to keep track of what messages this server has received
        self.message_bag = queue.Queue(maxsize=400)

        self.multicast_group = (MULTICAST_GROUP_ADDRESS, MULTICAST_GROUP_PORT)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.socket.settimeout(2.0)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self.modify_membership(group)
        self.handle_connection()
        
    def set_current_state(self):
        if self.group == None:
            self.current_state = "candidate"
        elif self.id == self.group.leader:
            self.current_state = "leader"
        elif self.id in self.group.followers:
            self.current_state = "follower"

    def handle_connection(self):
        """socket bind for leader, connect for follower"""
        self.socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, struct.pack('b', 1))
        if self.current_state != "leader":
            self.socket.bind(self.multicast_group)

    def modify_membership(self, membership):
        self.group = membership
        before_state = self.current_state
        self.set_current_state()
        if before_state != self.current_state:
            mreq = struct.pack('4sL', socket.inet_aton(self.multicast_group[0]), socket.INADDR_ANY)
            if self.current_state == "follower":
                self.socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            elif self.current_state == "candidate":
                self.socket.setsockopt(socket.SOL_IP, socket.IP_DROP_MEMBERSHIP, mreq)


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
        print(">> socket {0}".format(self.socket.getsockname()))
        if self.group:
            membership = repr(self.group)
            return membership
        else:
            return

    def send_msg(self, msg):
        """"""
        sent = self.socket.sendto(msg, self.multicast_group)
        return sent

    def send_ack(self, msg, server):
        """"""
        sent = self.socket.sendto(msg, server)
        return sent

    def recv_response(self):
        """"""
        try:
            message, server = self.socket.recvfrom(4096)
            received_message = pickle.loads(message)
            return received_message, server
        except socket.timeout:
            # print("timeout")
            return "timeout", None

    def multicast_membership(self):
        message = MembershipMessage(self.group.id, self.id, self.sequence_number, self.group, None)
        data_string = pickle.dumps(message)
        #pdb.set_trace()
        return_code = self.send_msg(data_string)
        self.sequence_number += 1
        return "multicast_sent"

    def receive(self):
        """Listens for multicast messsys.stderrages"""

        while True:
            response, server = self.recv_response()
            if response != "timeout":
                # Leader only receives acknowledgments
                if self.current_state == "Leader" and isinstance(response, AcknowledgementMessage):
                    # TODO: check all follower received response, otherwise resend
                    print(response.repr())
                else:
                    try:
                        if response.get_sequence_number() in self.received_messages[response.get_sender_id()]:
                            print("previously_received_message")
                            # return
                        
                    except KeyError:
                        self.received_messages[response.get_sender_id()] = []

                    # In any case
                    # Others handle every type of Message
                    if isinstance(response, AcknowledgementMessage):
                        # Should not receive Ack if not a leader, but lets just print
                        print(response.repr())
                    elif isinstance(response, MembershipMessage):
                        print(response.repr())
                        # Modify Membership
                        self.modify_membership(response.get_membership_dict())
                        # Acknowledge
                        if response.get_sender_id() != self.id:
                            confirmation = AcknowledgementMessage(self.group.id, self.id, self.sequence_number, response.repr())
                            data_string = pickle.dumps(confirmation)
                            if response.get_sender_id() == self.group.leader:
                                self.send_ack(data_string, server)
                            self.send_msg(data_string)
                            """for member in range(self.group.group_size() - 1):
                                confirmation = self.recv_response()
                                if confirmation == "timeout":
                                    print("multicast_failure")
                                    # return
                                self.received_messages[response.get_sender_id()].append(response.get_sequence_number())
                                    """
                        print(self.report_group_membership())
                    elif isinstance(response, Message):
                        print("What is this message?")
                    else:
                        print("Send a Message object")
                    
                    # return response



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
    def __init__(self, group_id, sender_id, sequence_number, data=None):
        self.__group_id = group_id
        self.__sender_id = sender_id
        self.__sequence_number = sequence_number
        self.__data = data

    def get_group_id(self):
        return self.__group_id

    def get_sender_id(self):
        return self.__sender_id

    def get_sequence_number(self):
        return self.__sequence_number

    def get_data(self):
        return self.__data

    def repr(self):
        return "Message sequence {} for group {} from {}.".format(self.__sequence_number,
            self.__group_id, self.__sender_id, self.__data)

class MembershipMessage(Message):
    def __init__(self, group_id, sender_id, sequence_number, membershipDict, data=None):
        super(MembershipMessage, self).__init__(group_id, sender_id, sequence_number, data)
        self.__membershipDict = membershipDict

    def get_membership_dict(self):
        return self.__membershipDict

    def repr(self):
        return super(MembershipMessage, self).repr() + 'Received Membership: {}'.format(self.__membershipDict)

class AcknowledgementMessage(Message):
    """docstring for AcknowledgementMessage"""
    def __init__(self, group_id, sender_id, sequence_number, data=None):
        super(AcknowledgementMessage, self).__init__(group_id, sender_id, sequence_number, data)
        #self.arg = arg
    
    def repr(self):
        return 'Sender {} acknowledges "{}".'.format(self.get_sender_id(), self.get_data())
