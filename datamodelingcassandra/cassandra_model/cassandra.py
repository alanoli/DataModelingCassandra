import numpy as np
from cassandra.cluster import Cluster
from cassandra_model.queries import *
import csv


class CassandraModel:
    """ """
    def __init__(self, input_file, server_instance='127.0.0.1', keyspace_name='udacity'):
        self.key_space_name = keyspace_name
        self.file = input_file

        self.cluster = self._create_cluster()
        self.session = self._open_session()
        self._create_keyspace()
        # self._model_query_1()
        self._model_query_2()
        # self._model_query_3()

    def _model_query_1(self):
        """ """
        try:
            self.session.execute(QUERY_CREATE_1)
            self._insert_data(QUERY_INSERT_1, [0, 9, 5, 8, 3])
        except Exception as e:
            print('Error on creating table for query 1. ' + str(e))
    
    def _model_query_2(self):
        """ """
        try:
            self.session.execute(QUERY_CREATE_2)
            self._insert_data(QUERY_INSERT_2, [0, 9, 8, 10, 1, 4, 3])
        except Exception as e:
            print('Error on creating table for query 2. ' + str(e))

    def _model_query_3(self):
        """ """
        pass

    def show_query_1(self, session_id, item_in_session):
        """ """
        try:
            rows = self.session.execute(QUERY_SELECT_1, (session_id, item_in_session))
            print('Query 1 results: \n')
            for row in rows:
                print(row.artist, row.song_title, row.song_length)
        except Exception as e:
            print('Error on querying data for QUERY 1. ' + str(e))

    def show_query_2(self, user_id, session_id):
        """ """
        try:
            rows = self.session.execute(QUERY_SELECT_2, (user_id, session_id))
            print('Query 2 results: \n')
            for row in rows:
                print(row.artist, row.song_title)
        except Exception as e:
            print('Error on querying data for QUERY 2. ' + str(e))

    def show_query_3(self):
        """ """
        pass

    def _insert_data(self, query, columns):
        """ """
        with open(self.file, encoding='utf8') as f:
            csvreader = csv.reader(f)
            next(csvreader) # skipping header
            for line in csvreader:
                line[5] = float(line[5])
                line[8] = int(line[8])
                line[3] = int(line[3])
                line[10] = int(line[10])
                self.session.execute(query, tuple(map(line.__getitem__, columns)))

    def _create_keyspace(self):
        """ """
        try:
            self.session.execute("""
                CREATE KEYSPACE IF NOT EXISTS udacity
                WITH REPLICATION = 
                {'class' : 'SimpleStrategy', 'replication_factor' : 1}"""
            )
            self.session.set_keyspace('udacity')
        except Exception as e:
            print('Error on creating keyspace. ' + str(e))

    def _create_cluster(self):
        """ """
        return Cluster()
    
    def _open_session(self):
        """ """
        return self.cluster.connect()

    def close_session(self):
        """ """
        self.session.execute(QUERY_DROP_ALL_TABLES)
        self.session.shutdown()
        self.cluster.shutdown()
