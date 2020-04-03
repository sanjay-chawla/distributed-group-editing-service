from log import *

def check_membership(conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM group_membership")

    rows = cur.fetchall()
    for row in rows:
        print(row)

def print_log(log):
    print("printing log records")
    filename = log.filename
    with open(filename, "r") as f:
        for line in f:
            print(line)


def log_test():
    server_id = 5
    log = Log(server_id)
    log.purge()
    check_point = CheckPoint(server_id, log)
    in_memory_db_conn = sqlite3.connect(":memory:")
    
    # set up the the database in memory
    membership_before = {"leader": 0, "followers": [1, 2, 3, 4, 5]}
    
    followers = str(membership_before["followers"])[1:-1].replace(",", "")
    leader = membership_before["leader"]
    in_memory_db_conn.execute('''
    CREATE TABLE IF NOT EXISTS group_membership AS
        SELECT "{0}" AS leader, "{1}" AS followers;
    '''.format(leader, followers))
    in_memory_db_conn.commit()
    # print the original membership
    print("orginal membership in memory")
    check_membership(in_memory_db_conn)
    print()

    # take a checkpoint of the original membership
    check_point.take_snapshot(log, in_memory_db_conn)

    sql_statement = """
    UPDATE group_membership
    SET leader = "3", followers = "0 1 2 4 5"
    """
    order = 0
    log.log_then_excute(sql_statement, order, in_memory_db_conn)
    print("after log_then_excute membership in memory")
    check_membership(in_memory_db_conn)
    print()
    print_log(log)

    check_point.restore(log, in_memory_db_conn)

    print("membership after checkpoint restore")
    check_membership(in_memory_db_conn)
    print()

    #close the database in memory connection
    in_memory_db_conn.close()
log_test()
