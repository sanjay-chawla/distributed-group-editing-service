#!/usr/bin/env python3
import sys
import atexit
from membership import Server, Group

#membership_before = {"leader": 0, "followers": [1, 2]}
#leader_num = membership_before["leader"]
#followers = membership_before["followers"]

def set_up_server(server_id, group_id, restore):
    """
    Set up a server depending on whether it is a leader/follower
    or not in the current group
    """
    # TODO: remove hardcoding
    server = Server(server_id, group_id, 3, restore)
    print("Setting up server {0} as {1}".format(server_id, server.current_state))
    return server

def main():
    if len(sys.argv) != 4:
        print('Usage is: {} <server_id> <group_id> <restore_from_db>'.format(sys.argv[0]))
        exit(1)
    server_id = int(sys.argv[1])
    group_id = None if int(sys.argv[2])==0 else int(sys.argv[2])
    restore = sys.argv[3] == "True"
    
    #is_leader, is_follower = server_id == leader_num, server_id in followers
    #group = Group(0, literal_eval(sys.argv[2]))
    #current_state = string(sys.argv[3])

    #server = Server(process_id, group, current_state)
    server = set_up_server(server_id, group_id, restore)
    atexit.register(server.exit)
    server.run()

if __name__ == '__main__':
    main()
