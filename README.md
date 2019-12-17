# DataModelingCassandra
Data modeling project with Apache Cassandra - Udacity Nanodegree program. In this project, the music app history information modeled to a NoSql database.

## Running
Before running, ensure you have the dataset on the 'event_data' folder.

To execute the project, simply run:

    python ./datamodelingcassandra/main.py

## Dependencies
The core of the project is the database - Cassandra. For running it locally, you must have an instance installed. Suggestion is to run a docker container on your local machine, using the Cassandra image, from Docker Hub:

https://hub.docker.com/_/cassandra

To get the python modules installed, run:

    pip install cassandra-driver

## NoSQL model and queries
In this project a NoSql modeling approach was used for storing data for later use. As of any Cassandra table modeling, the queries were defined first, then, the table and its model were created for suiting the queries needs.

Three queries are created in this demo project.