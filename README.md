# DATA WAREHOUSE ETL IN AWS REDSHIFT

## Introduction 

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Project objective 

This project focuses on understanding how to setup AWS Redshift cluster, and build an ETL for a database hosted on Redshift. 

## Dataset

- Song Dataset: a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.<br>
- Log Dataset: of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

## Schema for Song Play Analysis

The star schema is used in the project, it consists a main fact table with all variables associated with each event *songplays*, and 4 dimension tables *uses*, *songs*, *artists* and *time* with primary keys referenced from the fact table.

1. Fact table:
    - **songplays**:  records in log data associated with song plays: *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*.
2. Dimension Tables:
    - **users** - users in the app : *user_id, first_name, last_name, gender, level*.        
    - **songs** - songs in music database: *song_id, title, artist_id, year, duration*.      
    - **artists** - artists in music database: *artist_id, name, location, latitude, longitude*.      
    - **time** - timestamps of records in **songplays** broken down into specific units: *start_time, hour, day, week, month, year, weekday*.

## Project Instruction

1. create_tables.py: create your fact and dimension tables for the star schema in Redshift.<br>
2. etl.py: load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.<br>
3. sql_queries.py: define you SQL statements, which will be imported into the two other files above.


## Project Step
Here are two major steps in this project:
#### Create Table Schemas:
- Design schemas for fact and dimension tables
- Write a SQL CREATE statement for each of these tables in sql_queries.py
- Complete the logic in create_tables.py to connect to the database and create these tables
- Write SQL DROP statements to drop tables in the beginning of create_tables.py if the tables already exist. This way, you can run create_tables.py whenever you want to reset your database and test your ETL pipeline
- Launch a redshift cluster and create an IAM role that has read access to S3
- Add redshift database and IAM role info to dwh.cfg
- Test by running create_tables.py and checking the table schemas in your redshift database. You can use Query Editor in the AWS Redshift console for this

#### Build ETL Pipeline:
- Implement the logic in etl.py to load data from S3 to staging tables on Redshift
- Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift
- Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results
- Delete your redshift cluster when finished


## How to Run:
1. Run create_tables.py to create staging and dimension tables.<br>
2. Execute ETL process by running etl.py.