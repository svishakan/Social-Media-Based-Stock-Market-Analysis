import os
import csv
import string
import requests
from urllib import response
from pprint import PrettyPrinter
from dotenv import load_dotenv



def get_current_news_data(query, extra_params):
    """Returns the current news data fetched from NewsData.io API."""

    #Load API keys
    NEWSDATA_API_KEY = os.getenv("NEWSDATA_API_KEY_2")

    #NewsData.io URL
    url = "https://newsdata.io/api/1/news?apikey=" + NEWSDATA_API_KEY
    params = {"q" : query, "language" : "en"}

    params.update(extra_params)

    extracted_news = []

    while True:
        req = requests.get(url, params)
        response = req.json()

        for result in response["results"]:
            extracted_news.append(result["title"])

        #if response["nextPage"]:
            #params["page"] = response["nextPage"]

        else:
            break

    return extracted_news


def write_to_csv(data, file="NewsData.csv", category = "None"):
    """Writes the content with category to csv file to make the dataset."""

    filepath = os.path.join(os.path.dirname(__file__),f"../../Datasets/{file}")
    if os.path.exists(filepath):
        filemode = "a+"
    else:
        filemode = "w+"
    
    #with open(f"../Datasets/{file}", "a") as data_csv:
    with open(filepath, filemode) as data_csv:
    
        csv_writer = csv.writer(data_csv, delimiter='\t')
        
        for data_point in data:
            #remove non-printable characters
            data_point = ''.join(filter(lambda x: x in string.printable, data_point))
            #append to csv
            csv_writer.writerow([category, data_point])

        data_csv.close()


def main(file="NewsData.csv"):
    
    #Load .env file
    load_dotenv()

    keywords = {"Crypto": ["bitcoin", "dogecoin", "crypto"],
                "Oil": ["crude oil", "oil prices"],
                "EVs": ["tesla motors", "electric vehicle", "EVs"],
                "Gaming": ["XBox", "Playstation", "Video Games"],
                "Tech": ["Apple", "Facebook", "Google", "Amazon"]}


    print("Started collecting data from the APIs...")

    for category in keywords.keys():
        responses = set()
        extra_params = {}

        if category == "Tech":  #to restrict tech news to only tech articles
            extra_params["category"] = "technology"

        print("\n\t---Currently parsing the '{}' category...---".format(category))
        keywords_list = keywords[category]

        for keyword in keywords_list:
            print("\t\t---Currently parsing the '{}' keyword...---".format(keyword))
            response = get_current_news_data(keyword, extra_params)

            for resp in response:   #add to set to prevent duplicates
                responses.add(resp)

        write_to_csv(responses, file, category)

    print("Finished collecting data from the APIs.")