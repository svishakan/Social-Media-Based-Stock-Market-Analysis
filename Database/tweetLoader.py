# Import required modules
import csv
import sqlite3
import os

# Connecting to the FYP database
connection = sqlite3.connect(os.path.join(os.path.dirname(__file__), f'fypdb-Pharma.sqlite'))

# Creating a cursor object to execute SQL queries on a database table
cursor = connection.cursor()


FILENAME = "NewPharmaTweetData.csv"
TABLENAME = "pharma_tweets_count"

# Table Definition
create_table = f'''CREATE TABLE IF NOT EXISTS {TABLENAME}(
				category TEXT,
				tweetDate DATE,
				count INTEGER,
				tweet TEXT NOT NULL,
				CONSTRAINT uniq_twt PRIMARY KEY (category, tweetDate, tweet)
       			);
				'''
# Creating the table into our
# database
cursor.execute(create_table)

# Opening the NewTweetData.csv file
file = open(os.path.join(os.path.dirname(__file__),f"../Datasets/{FILENAME}"))
# Reading the contents of the NewTweetData.csv file
contents = csv.reader(file, delimiter='\t')

# SQL query to insert data into the tweets table
try:
	insert_records = f"INSERT INTO {TABLENAME} (category, tweetDate, count, tweet) VALUES(?, ?, ?, ?) ON CONFLICT(category, tweetDate, tweet) DO NOTHING"
 	# Importing the contents of the file into our tweets table
	cursor.executemany(insert_records, contents)
except sqlite3.Error as error:
    print({error})
    pass


# SQL query to retrieve all data from the tweets table to verify that the
# data of the csv file has been successfully inserted into the table
select_all = f"SELECT * FROM {TABLENAME}"
rows = cursor.execute(select_all).fetchall()

# Output to the console screen
print({"Rows written" : len(rows)})

# Committing the changes
connection.commit()

# closing the database connection
connection.close()