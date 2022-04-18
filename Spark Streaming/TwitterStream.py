import tweepy
from tweepy import OAuthHandler, Stream, Client
#from tweepy.streaming import StreamListener #deprecated
import socket
import json
import requests
import os
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("TWITTER_DEVELOPER_API_KEY")
api_key_secret = os.getenv("TWITTER_DEVELOPER_API_SECRET_KEY")
access_token = os.getenv("ACCESS_TOKEN")
access_token_secret = os.getenv("ACCESS_TOKEN_SECRET")
bearer_token = os.getenv("TWITTER_DEVELOPER_BEARER_TOKEN")

#Stream superclass only for Elevated API level access
#Client superclass for Basic API level access

class TweetListener(Client):
    def __init__(self, *args, csocket):
        super().__init__(*args) #Stream Class usage changed - check docs
        self.client_socket = csocket
    
    def on_data(self, data):
        try:
            msg = json.loads(data)
            print(msg['text'].encode('utf-8'))  #Remove emoji
            #print("\nHello")
            self.client_socket.send(msg['text'].encode('utf-8'))
            return True #success
        
        except BaseException as e:
            print("Error:", e)
        
        return True

    def on_error(self, status):
        print(status)
        return True

#New
def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    #print(json.dumps(response.json()))


def set_rules(delete):
    # You can adjust the rules if needed
    sample_rules = [
        {"value": "bitcoin"},     ##KEYWORD BASED RULES
    ]
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))


def get_stream(set, twitter_stream):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, stream=True,
    )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            data = json_response["data"]
            text = data["text"] 

            print(json.dumps(text, indent=4, sort_keys=True))
            twitter_stream.client_socket.send(text.encode('utf-8')) #Remove emojis
#New

def sendData(c_socket):
    #Deprecated
    #auth = OAuthHandler(consumer_key=API_KEY, consumer_secret=API_KEY_SECRET)
    #auth.set_access_token(access_token, access_token_secret)

    twitter_stream = TweetListener(
        bearer_token,
        api_key, api_key_secret,
        access_token, access_token_secret,
        csocket=c_socket)

    rules = get_rules()
    delete = delete_all_rules(rules)
    set = set_rules(delete)
    get_stream(set, twitter_stream)

    #twitter_stream.filter(track=['Chess'])



if __name__ == "__main__":
    s = socket.socket()
    host = "127.0.0.1"
    port = 9999
    s.bind((host, port))

    print("Listening on Port {}".format(port))

    s.listen(5) #Listen every 5 seconds
    c, addr = s.accept()    #Establish connection

    sendData(c)
