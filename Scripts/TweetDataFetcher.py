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
    extracted_news = []
    json_response = connect_to_endpoint(search_url, query_params)
    
    while page_count:
        page_count -= 1
        
        for tweet in json_response['data']:
            if tweet['lang'] == 'en':
                extracted_news.append(tweet['text'])
    
        if json_response['meta']['next_token']:
            query_params['next_token'] = json_response['meta']['next_token']
            json_response = connect_to_endpoint(search_url, query_params)
	#print(json.dumps(json_response, indent=4, sort_keys=True))
    return extracted_news

def write_to_csv(data, file="NewsData.csv", category = "None"):
    """Writes the content with category to csv file to make the dataset."""

    filepath = os.path.join(os.path.dirname(__file__),f"../Datasets/{file}")
    if os.path.exists(filepath):
        filemode = "a+"
    else:
        filemode = "w+"
    
    #with open(f"../Datasets/{file}", "a") as data_csv:
    with open(filepath, filemode) as data_csv:
    
        csv_writer = csv.writer(data_csv, delimiter='\t')
        
        for data_point in data:
            #remove non-printable characters
            data_point = ''.join(filter(lambda x: x in string.printable, data_point)).replace('\n', ' ').replace('\r', '').replace('\t', ' ').replace('&gt', ' ').replace('&lt', ' ')
            data_point = re.sub(r'\bhttps://t.co/[^ ]*\b',' ', data_point)
            data_point = ' '.join(data_point.split())
            #append to csv
            csv_writer.writerow([category, data_point])

        data_csv.close()

def main(file, page_count = 10):
    load_dotenv()
    
    
    global bearer_token 
    bearer_token = os.getenv("TWITTER_DEVELOPER_BEARER_TOKEN")
    global search_url
    search_url = "https://api.twitter.com/2/tweets/search/recent"

    keywords = {"Crypto": ["bitcoin", "dogecoin", "crypto"],
                "Oil": ["crude oil", "oil prices"],
                "EVs": ["tesla motors", "electric vehicle", "EVs"],
                "Gaming": ["XBox", "Playstation", "Video Games"],
                "Tech": ["Apple", "Facebook", "Google", "Amazon"]}
    
    query_params = {}
    query_params['tweet.fields'] = 'created_at,lang,source'
            
    for category in keywords.keys():
        responses = set()
        keywords_list = keywords[category]

        print(f'Collecting {category} tweets')
        
        for keyword in keywords_list:
            print(f'\tCollecting {keyword} tweets')
            # query_params['query'] = f"#{keyword}"
                
            # response = get_tweet_data(query_params)
            
            # for resp in response:   #add to set to prevent duplicates
            #     responses.add(resp)
            
            #query_params['query'] = f"{keyword} -horrible -worst -sucks -bad -disappointing -accident -fall -down"
            query_params['query'] = f"(-is:retweet {keyword} -has:links) OR ( -is:retweet #{keyword} -has:links)"
                
            response = get_tweet_data(query_params, page_count)
            
            for resp in response:   #add to set to prevent duplicates
                responses.add(resp)
            
            write_to_csv(responses, file, category)
        print()
    
if __name__ == "__main__":
    filename = "TweetData.csv"
    page_count = 10
    
    main(filename, page_count)