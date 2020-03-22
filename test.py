"""
Start with a group with leader 1 and followers [0, 2, 3, 4]
Server 5 wants to join the group
It sends a message to the leader and leader updates the group membership
The leader 1 sends the current group membership to all followers
Each follower creat such group using the message received and bind it to its group attribute
"""

import sys
from membership import *

membership_before = {"leader": 1, "followers": [0, 3]}
leader_num = membership_before["leader"]
followers = membership_before["followers"]
server_to_join = 5

def parse_command_line():
    """
    Return the server_id from the command line
    """
    if len(sys.argv) != 2:
        print("Usage: python test.py SEVER_ID")
        quit()
    else:
        return int(sys.argv[1])

server_id = parse_command_line()
is_leader, is_follower = server_id == leader_num, server_id in followers

def set_up_server(server_id, is_leader, is_follower):
    """
    Set up a server depending on whether it is a leader/follower
    or not in the current group
    """
    if is_leader:
        server = Server(server_id, Group(0, membership_before), "leader")
        state = "leader"
    elif is_follower:
        server = Server(server_id, Group(0, membership_before), "follower")
        state = "follower"
    else:
        server = Server(server_id)
        state = "candidate"
    server.handle_connection()
    print("Setting up server {0} as {1}".format(server_id, state))
    return server

server = set_up_server(server_id, is_leader, is_follower)
print(server.report_group_membership())

def leader_join_group(is_leader, server_to_join):
    if is_leader:
        server.group.join_group(server_to_join)
        print()
        print("******leader joins 5 to its group:******")
        print(server.report_group_membership())

def leader_multicast_msg(is_leader):
    if is_leader:
        message_to_send = server.report_group_membership()
        followers = server.group.followers
        server.multicast(message_to_send)

leader_join_group(is_leader, server_to_join)
leader_multicast_msg(is_leader)


print("Started listening for messages")
server.receive()

"""
while True:
    response = server.recv_response(is_print_wait_msg=False)
    if response != "timeout":
        pdb.set_trace()
        msg = pickle.loads(response[0])

        print()
        print("Message received as:")
        membership = eval(msg.get_data())
        print(membership)
        server.group = Group(membership)
        print(server.report_group_membership())
"""
