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
    cass_db.show_query_1(session_id=338, item_in_session=4)
    # cass_db.show_query_2()
    # cass_db.show_query_3()

    cass_db.close_session()

if __name__ == '__main__':
    main()
