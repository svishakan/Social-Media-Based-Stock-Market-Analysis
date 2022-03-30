# Import required modules
import csv
import sqlite3

# Connecting to the FYP database
connection = sqlite3.connect('fypdb.db')

# Creating a cursor object to execute SQL queries on a database table
cursor = connection.cursor()

# Table Definition
create_table = '''CREATE TABLE tweets(
				category TEXT,
				tweetDate DATE,
				tweet TEXT NOT NULL);
				'''

# Creating the table into our
# database
# cursor.execute(create_table)

# Opening the NewTweetData.csv file
file = open('../Datasets/NewTweetData.csv')

# Reading the contents of the NewTweetData.csv file
contents = csv.reader(file, delimiter='\t')

# SQL query to insert data into the tweets table
insert_records = "INSERT INTO tweets (category, tweetDate, tweet) VALUES(?, ?, ?)"

# Importing the contents of the file into our tweets table
cursor.executemany(insert_records, contents)

# SQL query to retrieve all data from the tweets table to verify that the
# data of the csv file has been successfully inserted into the table
select_all = "SELECT * FROM tweets"
rows = cursor.execute(select_all).fetchall()

# Output to the console screen
for r in rows:
	print(r)

# Committing the changes
connection.commit()

# closing the database connection
connection.close()