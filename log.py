
import os
import sqlite3

class Log(object):
    """
    A sql statement based implementation of the log replication
    Do not handle non-dertministic sql statements
    """
    def __init__(self, server_id):
        path = os.getcwd()
        self.filename = path + "/logs/server" + str(server_id) + ".log"
    
    def log_then_excute(self, sql_statement, order, in_memory_conn):
        """
        keep a log of
        INSERT, UPDATE and DELETE statements
        """
        # replace \n in sql_statement with an empty space
        sql_statement = sql_statement.replace("\n", " ")
        # append the log
        with open(self.filename, "a") as f:
            f.write(str(order) + "\t" + str(sql_statement) + "\n")
        # excute the sql statement after the log is recorded
        in_memory_conn.execute(sql_statement)
        in_memory_conn.commit()
    
    def purge(self):
        """Get rid of the logs"""
        open(self.filename, 'w').close()

class CheckPoint(object):
    """
    take a snapshot of the current group membership
    """
    def __init__(self, server_id, log):
        path = os.getcwd()
        self.filename = path + "/checkpoints/server" + str(server_id) + "_checkpoint.db"
        self.log = log

    def take_snapshot(self, log, in_memory_conn):
        """replicate the sql database to self.filename then get rid of the previous logs"""
        # replicate the current sql database to checkpoint
        bck = sqlite3.connect(self.filename)
        in_memory_conn.backup(bck)
        bck.close()
        self.log.purge()

    def restore(self, log, in_memory_conn):
        """restore the group membership before crashes using the latest checkpoin the logs"""
        # load last checkpoint
        bck = sqlite3.connect(self.filename)
        bck.backup(in_memory_conn)
        bck.close()
        print("membership from checkpoint")
        check_membership(in_memory_conn)
        print()
        # excute sql statements in the log
        # TODO
        with open(self.log.filename, "r") as f:
            for line in f:
                tab_postition = line.find("\t")
                sql_statement = line[tab_postition+1:]
                in_memory_conn.execute(sql_statement)


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