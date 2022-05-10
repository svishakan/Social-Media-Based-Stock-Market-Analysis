import sqlite3
import os

TABLENAME = "oil_tweets_count"


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

    create_table = """
                    CREATE TABLE IF NOT EXISTS tweet_data(
                    category TEXT,
                    ticker TEXT,
                    tweetDate DATE,
                    count INTEGER,
                    tweet TEXT NOT NULL,
                    CONSTRAINT uniq_twt_dt PRIMARY KEY (category,  ticker, tweetDate, tweet)
                    );"""



    cursor.execute(create_table);
    connection.commit();

    select_all = f"SELECT * FROM {TABLENAME}"

    rows = cursor.execute(select_all).fetchall()

    insert_records = [];
    insert_query = f"INSERT INTO tweet_data (category, ticker, tweetDate, count, tweet) VALUES(?, ?, ?, ?, ?) ON CONFLICT(category, ticker, tweetDate, tweet) DO NOTHING"
    
    for row in rows:
        tickers = find_tickers(row[0], row[3]);
        for ticker in tickers:
            insert_records.append((row[0], ticker, row[1], row[2], row[3]));
    
    cursor.executemany(insert_query, insert_records)
    connection.commit()
    