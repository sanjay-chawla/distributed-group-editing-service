# Distributed Group Editing Service

An effort to implement a distributed text editor. All nodes are connected via UDP multicast communication. RAFT is used for consensus group management. Additionally, checkpoints store states in SQL Lite DB file and can be used to restore last state of the node.

## Running test
1. We suggest using Terminator which allows multiple terminals to be accessed in a single window
2. Setup desired group of nodes
3. Run `./total_order_multicast.py <server_number> <restore_earlier_state>`
    > You must run with restore False first to create a checkpoint, then use True to restore state
4. Enter message in any terminal and other receive in same order
5. Membership can be changes on the fly with `mem <group_id> <leader> [<follower1,follower2,..>]`

## Outputs
Outputs are in screenshots (.png) files. Logs and checkpoint DB files are created in respective folders
