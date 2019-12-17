import numpy as np
from cassandra.cluster import Cluster
from cassandra_model.queries import *
import csv


class CassandraModel:
    """
    Defines an object of a cluster in Cassandra, opening the session for interaction
    with the database. Also, it manages the query and table modeling on this instance.
    At the end, remember calling the close_session() method.
    """
    def __init__(self, input_file, server_instance='127.0.0.1', keyspace_name='udacity'):
        self.key_space_name = keyspace_name
        self.file = input_file

        self.cluster = self._create_cluster(server_instance)
        self.session = self._open_session()
        self._create_keyspace()

        # Modeling the tables requested
        self._model_table(QUERY_CREATE_1, QUERY_INSERT_1, parameters=[0, 9, 5, 8, 3])
        self._model_table(QUERY_CREATE_2, QUERY_INSERT_2, parameters=[0, 9, 8, 10, 1, 4, 3])
        self._model_table(QUERY_CREATE_3, QUERY_INSERT_3, parameters=[1, 4, 9])

    def _model_table(self, create_table_query, insert_query, parameters):
        """Creates the table modeled by the query requested using the CREATE table statement"""
        try:
            self.session.execute(create_table_query)
            self._insert_data(insert_query, parameters)
        except Exception as e:
            print('Error on creating table for query. ' + str(e))

    def exec_query(self, query_number, parameters):
        """This is run by the user to query data from the tables.
        To use it, one has to know the query and its parameters."""
        query = self._get_query_by_number(query_number)
        try:
            if not isinstance(parameters, tuple): # Handling single-valued tuple
                parameters = (parameters,)
            return self.session.execute(query, parameters)
        except Exception as e:
            print('Error on query data. ' + str(e))

    def _get_query_by_number(self, number):
        """Returns the respective query by its number"""
        if(number == 1):
            return QUERY_SELECT_1
        if(number == 2):
            return QUERY_SELECT_2
        if(number == 3):
            return QUERY_SELECT_3

    def _insert_data(self, query, columns):
        """Inserts data based on a INSERT query and a given set of columns"""
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
        """Creates the keyspace for storing the data"""
        try:
            self.session.execute("""
                CREATE KEYSPACE IF NOT EXISTS udacity
                WITH REPLICATION = 
                {'class' : 'SimpleStrategy', 'replication_factor' : 1}"""
            )
            self.session.set_keyspace('udacity')
        except Exception as e:
            print('Error on creating keyspace. ' + str(e))

    def _create_cluster(self, server_instance):
        """Creates and returns a cluster"""
        return Cluster([server_instance])
    
    def _open_session(self):
        """Opens and returns the session on the cluster"""
        return self.cluster.connect()

    def close_session(self):
        """Drops tables and closes session"""
        self.session.execute(QUERY_DROP_TABLE_1)
        self.session.execute(QUERY_DROP_TABLE_2)
        self.session.execute(QUERY_DROP_TABLE_3)
        self.session.shutdown()
        self.cluster.shutdown()
