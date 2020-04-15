
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
        # excute sql statements in the log
        with open(self.log.filename, "r") as f:
            for line in f:
                tab_postition = line.find("\t")
                sql_statement = line[tab_postition+1:]
                print(">> Executing from log: {}".format(sql_statement))
                in_memory_conn.execute(sql_statement)

    def fetch_latest_state(self, in_memory_conn):
        cursor = in_memory_conn.cursor()
        cursor.execute("SELECT * FROM group_membership ORDER BY id DESC LIMIT 1")
        result = cursor.fetchone()
        group_id = int(result[1])
        leader = int(result[2])
        if len(result[3].replace("[","").replace("]","").split(",")) != 0:
            followers = list(map(int, result[3].replace("{","").replace("}","").split(",")))
        expected_sequence_counter = int(result[4])
        term = int(result[5])
        return group_id, leader, followers, expected_sequence_counter, term
