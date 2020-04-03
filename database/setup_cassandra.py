"""
n  by Techfossguru
Copyright (C) 2017  Satish Prasad

"""
import logging
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, BatchStatement
from cassandra.query import SimpleStatement
import csv
import requests 
import pandas as pd

class PythonCassandraExample:

    def __init__(self):
        self.cluster = None
        self.session = None
        self.keyspace = None
        self.log = None

    def __del__(self):
        self.cluster.shutdown()

    def createsession(self):
        self.cluster = Cluster(['localhost'])
        self.session = self.cluster.connect(self.keyspace)

    def getsession(self):
        return self.session

    # How about Adding some log info to see what went wrong
    def setlogger(self):
        log = logging.getLogger()
        log.setLevel('INFO')
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        log.addHandler(handler)
        self.log = log

    # Create Keyspace based on Given Name
    def createkeyspace(self, keyspace):
        """
        :param keyspace:  The Name of Keyspace to be created
        :return:
        """
        # Before we create new lets check if exiting keyspace; we will drop that and create new
        rows = self.session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        if keyspace in [row[0] for row in rows]:
            self.log.info("dropping existing keyspace...")
            self.session.execute("DROP KEYSPACE " + keyspace)

        self.log.info("creating keyspace...")
        self.session.execute("""
                CREATE KEYSPACE %s
                WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
                """ % keyspace)

        self.log.info("setting keyspace...")
        self.session.set_keyspace(keyspace)

    def create_table(self):
        c_sql = """
                CREATE TABLE IF NOT EXISTS total_cases (curr_date date PRIMARY KEY,
                                              world_cases int, afghanistan_cases int, albania_cases int, algeria_cases int);
                 """

        self.session.execute(c_sql)
        self.log.info("Corona Virus Table Created !!!") 



        #options = ["afghanistan_cases", "albania_cases", "algeria_cases"]
        #for option in options:
            #c_sql_alter = """ ALTER TABLE total_cases ADD new %s int""" 
            # self.session.execute(c_sql_alter, option)
            #self.log.info("added : ")

    # lets do some batch insert


    def get_csv_info(self, link):
        '''df = pd.read_csv(link)
        names = df.column_name
        print(names)'''
        response = requests.get(link)
        reader = csv.DictReader(response.iter_lines())
        print(reader.fieldnames)
        #for row in reader:
            #headers = d_reader.fieldnames
            #print(row)


    def insert_data(self):
	insert_sql = self.session.prepare("INSERT INTO  total_cases (curr_date, world_cases, afghanistan_cases , albania_cases , algeria_cases) VALUES (?,?,?,?,?)")
        options = ["curr_date", "world_cases", "afghanistan_cases", "albania_cases", "algeria_cases"]
        #for option in options:
            #print(option)
            #insert_sql = self.session.prepare("INSERT INTO total_cases (option) VALUES (?)")
        batch = BatchStatement()
        batch.add(insert_sql, (12/30/19, 10, 2555, 12, 18))
        batch.add(insert_sql, (12/31/19, 1,2,3,4))
        batch.add(insert_sql, (1/1/20, 2,3,4,5))
        batch.add(insert_sql, (1/2/29, 3,4,5,6))
        """insert_sql = self.session.prepare("INSERT INTO  employee (emp_id, ename , sal,city) VALUES (?,?,?,?)")
        batch = BatchStatement()
        batch.add(insert_sql, (1, 'LyubovK', 2555, 'Dubai'))
        batch.add(insert_sql, (2, 'JiriK', 5660, 'Toronto'))
        batch.add(insert_sql, (3, 'IvanH', 2547, 'Mumbai'))
        batch.add(insert_sql, (4, 'YuliaT', 2547, 'Seattle'))"""
        self.session.execute(batch)
        self.log.info('Batch Insert Completed')

    def select_data(self):
        rows = self.session.execute('select * from total_cases limit 5;')
        for row in rows:
            print(row.curr_date, row.world_cases)

    def update_data(self):
        pass

    def delete_data(self):
        pass


if __name__ == '__main__':
    example1 = PythonCassandraExample()
    example1.createsession()
    example1.setlogger()
    example1.createkeyspace('corona_virus_keyspace')
    example1.create_table()
    example1.insert_data()
    example1.select_data()
    example1.get_csv_info('https://covid.ourworldindata.org/data/ecdc/total_cases.csv')

