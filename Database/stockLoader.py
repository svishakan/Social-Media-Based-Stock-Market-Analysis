# Import required modules
import csv
import sqlite3
import os

# Connecting to the FYP database
connection = sqlite3.connect(os.path.join(os.path.dirname(__file__), 'fypdb-PnF.sqlite'))

# Creating a cursor object to execute SQL queries on a database table
cursor = connection.cursor()

# Table Definition
# CATEGORY	TICKER	DATE	OPEN	CLOSE	PRE_MARKET	AFTER_HOURS	HIGH	LOW
create_table = '''CREATE TABLE IF NOT EXISTS stocks(
				category TEXT,
				ticker TEXT,
				stockDate DATE,
				open REAL,
				close REAL,
				pre_market REAL,
				afterHours REAL,
				high REAL,
				low REAL);
				'''

# Creating the table into our database
cursor.execute(create_table)

# Opening the StockData.csv file
file = open(os.path.join(os.path.dirname(__file__), '../Datasets/FoodStockData.csv'))

# Reading the contents of the StockData.csv file
contents = csv.reader(file, delimiter='\t')

# SQL query to insert data into the stocks table
insert_records = "INSERT INTO stocks (category, ticker, stockDate, open, close, pre_market, afterHours, high, low) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)"

# Importing the contents of the file into our stocks table
cursor.executemany(insert_records, contents)

# SQL query to retrieve all data from the stocks table to verify that the
# data of the csv file has been successfully inserted into the table
select_all = "SELECT * FROM stocks"
rows = cursor.execute(select_all).fetchall()

# Output to the console screen
for r in rows:
	print(r)

# Committing the changes
connection.commit()

# closing the database connection
connection.close()