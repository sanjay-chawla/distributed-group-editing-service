#!/usr/bin/env python3
import sys
from membership import Server, Group
from ast import literal_eval

membership_before = {"leader": 0, "followers": [1, 2]}
leader_num = membership_before["leader"]
followers = membership_before["followers"]

def set_up_server(server_id, is_leader, is_follower):
    """
    Set up a server depending on whether it is a leader/follower
    or not in the current group
    """
    if is_leader:
        server = Server(server_id, Group(0, membership_before))
        state = "leader"
    elif is_follower:
        server = Server(server_id, Group(0, membership_before))
        state = "follower"
    else:
        server = Server(server_id)
        state = "candidate"
    print("Setting up server {0} as {1}".format(server_id, state))
    return server

def main():
    if len(sys.argv) != 2:
        #print('Usage: {} [process ID] [group] [process_state]'.format(sys.argv[0]))
        print('Usage: {} [process ID]'.format(sys.argv[0]))
        exit(1)

    server_id = int(sys.argv[1])
    is_leader, is_follower = server_id == leader_num, server_id in followers
    #group = Group(0, literal_eval(sys.argv[2]))
    #current_state = string(sys.argv[3])

    #server = Server(process_id, group, current_state)
    server = set_up_server(server_id, is_leader, is_follower)
    server.run()

if __name__ == '__main__':
    main()
