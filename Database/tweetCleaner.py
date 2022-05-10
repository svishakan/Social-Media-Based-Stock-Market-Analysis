import sqlite3
import os

TABLENAME = "stock_data"
CATEGORY = "stock"

def find_tickers(category, tweet):
    
    tickers = []
    
    keyword_map = {
        "Tech": ["GOOGL", "AAPL", "INTC", "MSFT"],
        "Oil": ["XOM", "COP", "DVN", "ENB", "CVX"],
        "EVs": ["TSLA", "LCID"],
        "Gaming": ["EA", "ATVI"]
    }
    
    ticker_map = {
        "GOOGL": ["#GOOG", "#GOOGL", "$GOOG", "$GOOGL"],
        "AAPL": ["#AAPL", "$AAPL"],
        "INTC": ["#INTC", "$INTC"],
        "MSFT": ["#MSFT", "$MSFT"],
        "LCID": ["#LCID", "$LCID"],
        "TSLA": ["#TSLA", "$TSLA"],
        "EA": ["$EA"],
        "ATVI": ["#ATVI", "$ATVI"],
        "CVX": ["#CVX", "$CVX"],
        "XOM": ["#XOM", "$XOM"],
        "COP": ["#COP", "$COP"],
        "DVN": ["#DVN", "$DVN"],
        "ENB": ["#ENB", "$ENB"]
    }
    
    for keyword in keyword_map.keys():
        if keyword == category:
            
            for ticker in keyword_map[keyword]:
                for keyval in ticker_map[ticker]:
                    if(tweet.find(keyval) != -1):
                        tickers.append(ticker)
                                            
    return tickers

if __name__ == "__main__":

    connection = sqlite3.connect(os.path.join(os.path.dirname(__file__),f"../Database/fypdb.sqlite"))
    cursor = connection.cursor()
    
    connection1 = sqlite3.connect(os.path.join(os.path.dirname(__file__),f"../Database/fypdb-{CATEGORY}.sqlite"))
    cursor1 = connection1.cursor()
    
    create_table = """
                    CREATE TABLE IF NOT EXISTS stock_data(
                    category TEXT,
                    ticker TEXT,
                    stockDate DATE,
                    open REAL,
                    close REAL,
                    high REAL,
                    low REAL
                    );"""



    cursor1.execute(create_table);
    connection1.commit();

    select_all = f"SELECT * FROM {TABLENAME} "

    rows = cursor.execute(select_all).fetchall()

    insert_records = [];
    insert_query = f"INSERT INTO stock_data VALUES(?, ?, ?, ?, ?, ? , ?)"
    
    for row in rows:
        insert_records.append((row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
    
    cursor1.executemany(insert_query, insert_records)
    connection1.commit()
    