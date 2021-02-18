# Data Modeling with Cassandra - Udacity Data Engineer Nanodegree

## Project: Data Modeling with Cassandra

### Context 

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team from Sparkify to create the results.

### Project Description

In this project, you'll apply what you've learned on data modeling with Apache Cassandra and complete an ETL pipeline using Python. To complete the project, you will need to model your data by creating tables in Apache Cassandra to run queries. You are provided with part of the ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables.

We have provided you with a project template that takes care of all the imports and provides a structure for ETL pipeline you'd need to process this data.


### Below there is a descriptions for implemented tables with Apache Cassandra:

#### table: session_songs
##### 1. Designing database model in Apache Cassandra we need to think Queries first - in our scenario our query will look like: 
`SELECT artist, song, song_length FROM session_songs WHERE sessionId = 338 AND itemInSession = 4`
##### 2. Our <em>PRIMARY KEY</em> will consist of: 
###### Partition key: `sessionId`
###### Clustering Key `itemInSession`
###### That will allow us filter by these attributes.
##### 3. Our table will contain the following tables: 
- `sessionId INT PK`
- `itemInSession INT PK`
- `artist TEXT` 
- `song TEXT`
- `song_length FLOAT`
    
#### table: user_songs
##### 1. Designing database model in Apache Cassandra we need to think Queries first - in our scenario our query will look like: 
`SELECT itemInSession, artist, song, user_firstname, user_lastname FROM user_songs WHERE userId = 10 AND sessionId = 182`
##### 2. Our <em>COMPOSITE PRIMARY KEY</em> will consist of: 
###### Partition key: `userId`
###### Partition key: `sessionId`
###### Clustering Key `itemInSession`
###### That will allow us filter by these attributes.
##### 3. Our table will contain the following tables: 
- `userId INT PK`
- `sessionId INT PK`
- `artist TEXT` 
- `song TEXT`
- `user_firstname TEXT`
- `user_lastname TEXT`
- `itemInSession INT PK`  
    
	
#### table: app_history
##### 1. Designing database model in Apache Cassandra we need to think Queries first - in our scenario our query will look like: 
`SELECT user_firstname, user_lastname FROM app_history WHERE song = 'All Hands Against His Own'`
##### 2. Our <em>PRIMARY KEY</em> will consist of: 
###### Partition key: `song`
###### Clustering Key `userId`
###### That will allow us filter by these attributes.
##### 3. Our table will contain the following tables: 
- `song INT PK`
- `user_firstname TEXT` 
- `user_lastname TEXT`
- `userId INT PK`	

### Project structure:

1. ___Project_1B_ Project_Template___ - main file with Python code - ETL process
2. ___event_datafile_new.csv___ - csv file with event's data
3. ___images___ - directory with static files (images)
4. ___event_data___ - directory with raw data files



### Project steps:

##### ETL process has two parts:

1. Part I. ETL Pipeline for Pre-Processing the Files
2. Part II. Complete the Apache Cassandra coding portion of your project. 



### How to start project?

To run ETL process we need to run cells with Jupyter Notebook.



