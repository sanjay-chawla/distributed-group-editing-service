# Distributed Group Editing Service

An effort to implement a distributed text editor. All nodes are connected via UDP multicast communication. RAFT is used for consensus group management. Additionally, checkpoints store states in SQL Lite DB file and can be used to restore last state of the node.

## Running test
1. We suggest using Terminator which allows multiple terminals to be accessed in a single window
2. Install Colorama: `pip install colorama` and Tkinter: `sudo apt-get install python3-tk`
3. Setup desired group of nodes (script or manually)
4. Run `./total_order_multicast.py <server_number> <group_number> [<restore_earlier_state>]`
    > <restore_earlier_state> is default to False if not given. Use True to restore state
    > <group_number> 0 indicates a client sending messages, other numbers are used a group identifiers
6. Enter message in any client terminal and other workers should receive messages in same order (totally ordered multicast)
7. New followers can be added on the fly by using commands in step 4.

## Outputs
Outputs are in screenshots (.png) files. Logs and checkpoint DB files will be created in respective folders but not added to repo
