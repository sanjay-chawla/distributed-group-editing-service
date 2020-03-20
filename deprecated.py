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