#!/usr/bin/env python3
import sys
import atexit
from membership import Server, Group

membership_before = {"leader": 0, "followers": [1, 2]}
leader_num = membership_before["leader"]
followers = membership_before["followers"]

def main():

    server_id, is_restore = parse_command_line_args()
    is_leader, is_follower = server_id == leader_num, server_id in followers
    #group = Group(0, literal_eval(sys.argv[2]))
    #current_state = string(sys.argv[3])

    #server = Server(process_id, group, current_state)
    server = set_up_server(server_id, is_leader, is_follower, is_restore)
    atexit.register(server.exit)
    server.run()

def parse_command_line_args():
    """
    if 1 arguments is taken, server = argv[1], is_store = False
    if 2 arguments are taken, server = argv[1], is_store = argv[2] == 'True'
    """
    if len(sys.argv) == 2:
        server_id = int(sys.argv[1])
        is_restore = False
    elif len(sys.argv) == 3:
        server_id = int(sys.argv[1])
        is_restore = sys.argv[2] == "True"
    else:
        print('Usage is: {} <server_id> [<restore_from_db>]'.format(sys.argv[0]))
        exit(1)
    return server_id, is_restore



def set_up_server(server_id, is_leader, is_follower, restore):
    """
    Set up a server depending on whether it is a leader/follower
    or not in the current group
    """
    server = Server(server_id, group_id=0, leader = leader_num, followers = followers, restore = restore)
    print("Setting up server {0} as {1}".format(server_id, server.current_state))
    return server



if __name__ == '__main__':
    main()
