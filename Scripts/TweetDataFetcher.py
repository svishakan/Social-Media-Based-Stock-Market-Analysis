import re
from urllib import response
from numpy import extract
import requests
import os
import json
from dotenv import load_dotenv
import csv
import string
import requests
from urllib import response
import datetime

# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'

# Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
# expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields
bearer_token = None
search_url = None

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r


def connect_to_endpoint(url, params):
    response = requests.request("GET", search_url, auth=bearer_oauth, params=params)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def get_tweet_data(query_params, page_count = 10):
    extracted_tweets = []
    json_response = connect_to_endpoint(search_url, query_params)
    
    while page_count:
        page_count -= 1
        
        if 'data' in json_response:
            for tweet in json_response['data']:
                if tweet['lang'] == 'en':
                    extracted_tweets.append(tweet['text'])
        
        if 'meta' in json_response:
            if 'next_token' in json_response['meta']:
                query_params['next_token'] = json_response['meta']['next_token']
                json_response = connect_to_endpoint(search_url, query_params)
	#print(json.dumps(json_response, indent=4, sort_keys=True))
    
    print(f'\t\t -- Extracted {len(extracted_tweets)} tweets')
    return extracted_tweets

def write_to_csv(data, file="NewsData.csv", category = "None", date=datetime.datetime.now()):
    """Writes the content with category to csv file to make the dataset."""

    filepath = os.path.join(os.path.dirname(__file__),f"../Datasets/{file}")
    if os.path.exists(filepath):
        filemode = "a+"
    else:
        filemode = "w+"
    
    #with open(f"../Datasets/{file}", "a") as data_csv:
    with open(filepath, filemode) as data_csv:
    
        csv_writer = csv.writer(data_csv, delimiter='\t')
        
        if(filemode == "w+"):
            csv_writer.writerow(["CATEGORY", "DATE", "TWEET"])
        
        for data_point in data:
            #remove non-printable characters
            data_point = ''.join(filter(lambda x: x in string.printable, data_point)).replace('\n', ' ').replace('\r', '').replace('\t', ' ').replace('&gt', ' ').replace('&lt', ' ')
            data_point = re.sub(r'\bhttps://t.co/[^ ]*\b',' ', data_point)
            data_point = ' '.join(data_point.split())
            #append to csv
            csv_writer.writerow([category, date.strftime("%Y-%m-%d"), data_point])

        data_csv.close()

def main(file, page_count = 10):
    load_dotenv()
    
    
    global bearer_token 
    bearer_token = os.getenv("TWITTER_DEVELOPER_ACAD_BEARER_TOKEN")
    global search_url
    search_url = "https://api.twitter.com/2/tweets/search/all"

    old_keywords = {"Crypto": ["bitcoin", "dogecoin", "crypto"],
                "Oil": ["crude oil", "oil prices"],
                "EVs": ["tesla motors", "electric vehicle", "EVs"],
                "Gaming": ["XBox", "Playstation", "Video Games"],
                "Tech": ["Apple", "Facebook", "Google", "Amazon"]}

    keywords = { #"Crypto": ["BTC", "ETH"],
                "Oil": ["CVX", "XOM", "COP"],
                "EVs": ["TSLA", "LCID"],
                "Gaming": ["EA", "ATVI"],
                "Tech": ["AAPL", "GOOGL", "INTC", "FB"]}
    
    
    next_date = datetime.datetime(2020, 3, 30)
    
    end_date = datetime.datetime(2022, 3, 20)
    
    query_params = {}
    query_params['tweet.fields'] = 'created_at,lang,source'
    
    while(next_date < end_date):
        
        query_params['start_time'] = f'{next_date.strftime("%Y-%m-%d")}T00:00:00Z'
        query_params['end_time'] = f'{next_date.strftime("%Y-%m-%d")}T23:59:59Z'

        print(f"Fetching data for {next_date.strftime('%Y-%m-%d')}")
        
        for category in keywords.keys():
            responses = set()
            keywords_list = keywords[category]

            print(f'\tCollecting {category} tweets')
            
            for keyword in keywords_list:
                print(f'\t\tCollecting {keyword} tweets')
                
                query_params['query'] = f"( -is:retweet #{keyword} -has:links)"
                    
                response = get_tweet_data(query_params, page_count)
                
                for resp in response:   #add to set to prevent duplicates
                    responses.add(resp)
                
                write_to_csv(responses, file, category, next_date)
            print()
                
        next_date = next_date + datetime.timedelta(days=1)

    
if __name__ == "__main__":
    filename = "NewTweetData.csv"
    page_count = 10
    
    main(filename, page_count)