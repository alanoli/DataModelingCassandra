from etl.file_processing import Pipeline
from cassandra_model.cassandra import CassandraModel
import os


def main():
    """
        This runs the 'Data modeling with Cassandra' project.
        This file runs the ETL portion of the project, gathering data from the flat files inside the data folder.
        Also, it creates and models the data to a Cassandra database.
        Finally, it queries the data inserted.
    """
    
    # Makes the ELT processing on the files
    try:
        Pipeline(os.getcwd() + '/event_data', output_file='event_datafile_new.csv')
    except Exception as e:
        print('There was a problem on processing the folder. ' + str(e))
        return

    # Models data to Cassandra, using pre-processed flat files
    try:
        cass_db = CassandraModel(input_file='event_datafile_new.csv')
    except Exception as e:
        print('Could not model data to Cassandra db. ' + str(e))
        return

    # Queries data based on the three questions asked
    try:
        result_1 = cass_db.exec_query(1, (338, 4))
        result_2 = cass_db.exec_query(2, (10, 182))
        # result_2 = cass_db.exec_query(2, (10, 9)) # teste
        result_3 = cass_db.exec_query(3, ('Pump It'))

        print('Showing results for query 1:')
        for row in result_1:
            print(row.artist, row.song_title, row.song_length)

        print('\nShowing results for query 2:')
        for row in result_2:
            print(row.artist, row.song_title)
        
        print('\nShowing results for query 3:')
        for row in result_3:
            print(row.first_name, row.last_name)

    except Exception as e:
        print('Error on querying data on database. ' + str(e))

    cass_db.close_session()

if __name__ == '__main__':
    main()
