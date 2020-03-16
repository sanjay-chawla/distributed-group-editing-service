import socket
import struct
import sys
import pdb
import pickle

MULTICAST_GROUP_ADDRESS = '224.3.29.71'
MULTICAST_GROUP_PORT = 10000

class Server(object):
    def __init__(self, id, group = None):
        self.id = id
        self.group = group
        self.multicast_group = (MULTICAST_GROUP_ADDRESS, MULTICAST_GROUP_PORT)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.settimeout(0.2)
        self.socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, struct.pack('b', 1))
        #self.state = ("leader", "follower", "candidate")

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
        if self.group:
            membership = repr(self.group)
            return membership
        else:
            return

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
    def __init__(self, membership):
        self.leader = membership["leader"]
        self.followers = list(membership["followers"])

    def join_group(self, server):
        if server not in self.followers:
            self.followers.append(server)

    def leave_group(self, server):
        if server in self.followers:
            self.followers.remove(server)

    def __repr__(self):
        return '{' + '"leader": {0}, "followers": {1}'.format(str(self.leader), str(self.followers)) + '}'


# Assmue at most 6 servers
msgs = ["" for i in range(6)] # initilise a list of 6 empty strings

def multicast(sender, msg):
    """"""
    responses = []
    try:
        # Send data to the multicast group
        return_code = send_msg(sender, msg)

        # Look for responses from all recipients
        while True:
            response = recv_response(sender)
            if response == "timeout":
                break
            responses.append(response)
    return responses

def send_msg(sender, msg):
    """"""
    print ("{}, sending '{}'".format(sys.stderr, msg))
    msg = Message(sender.group, sender.id, msg)
    msg = pickle.dumps(msg)
    sent = sender.socket.sendto(msg, sender.multicast_group)
    return sent

def recv_response(sender):
    """"""
    print("{}, waiting to receive".format(sys.stderr))
    try:
        data, server = sender.socket.recvfrom(16)
        print("{}, received '{}' from {}".format(sys.stderr, data, server))
        return data, server
    except socket.timeout:
        print("{}, timed out, no more responses".format(sys.stderr))
        return "timeout"

def join_group_send_and_recv_test():
    """
    Start with a group with leader 1 and followers [0, 2, 3, 4]
    Server 5 wants to join the group
    It sends a message to the leader and leader updates the group membership
    The leader 1 replies the current group membership
    Server 5 creates such group using the membership and binds it to its group

    >>> membership_at_leader = {"leader": 1, "followers": [0, 2, 3, 4]}
    >>> leader = Server(1, Group(membership_at_leader))
    >>> leader.report_group_membership()
    Server 1's view of group memembership:
    '{"leader": 1, "followers": [0, 2, 3, 4]}'
    >>> leader.group.join_group(5)
    >>> leader.report_group_membership()
    Server 1's view of group memembership:
    '{"leader": 1, "followers": [0, 2, 3, 4, 5]}'
    >>> message_to_send = leader.report_group_membership()
    Server 1's view of group memembership:
    >>> pseudo_send_msg(1, 5, message_to_send)
    >>> message_recevied = pseudo_recv_msg(5)
    >>> membership = eval(message_recevied)
    >>> server5 = Server(5)
    >>> server5.group = Group(membership)
    >>> server5.report_group_membership()
    Server 5's view of group memembership:
    '{"leader": 1, "followers": [0, 2, 3, 4, 5]}'
    """

def join_group_multicast_test():
    """
    Start with a group with leader 1 and followers [0, 2, 3, 4]
    Server 5 wants to join the group
    It sends a message to the leader and leader updates the group membership
    The leader 1 sends the current group membership to all followers
    Each follower creat such group using the message received and bind it to its group attribute
    """
    # Set up the scenes with leader 1 and followers [0, 2, 3, 4]
    membership_before = {"leader": 1, "followers": [0, 2, 3, 4]}
    leader_num = 1
    followers = [0, 2, 3, 4]

    # Before join group
    servers = dict()
    print("******Before join group:******")
    for i in [leader_num] + followers:
        servers[i] = Server(i, Group(membership_before))
        print(servers[i].report_group_membership())
    servers[5] = Server(5)
    print(servers[5].report_group_membership())
    print()

    # join a group member in the leader
    print("******leader joins 5 to its group:******")
    servers[leader_num].group.join_group(5)
    for i in range(6):
        print(servers[i].report_group_membership())
    print()

    # leader sends a message to its updated followers
    message_to_send = servers[leader_num].report_group_membership()
    followers = servers[leader_num].group.followers
    pseudo_multicast(leader_num, followers, message_to_send)

    # mimic each followers receives such message at update its group attribute
    print("******After the message is received******")
    messages_received = dict()
    for i in followers:
        messages_received[i] = pseudo_recv_msg(i)
        membership = eval(messages_received[i])
        servers[i].group = Group(membership)
        print(servers[i].report_group_membership())

join_group_multicast_test()

class Message:

    def __init__(self, group_id, member_id, data=None):
        self.group_id = group_id
        self.member_id = member_id
        self.data = data

    def get_group_id(self):
        return self.group_id

    def get_message_type(self):
        return self.type

    def get_member_id(self):
        return self.member_id

    def get_data(self):
        return self.__data
