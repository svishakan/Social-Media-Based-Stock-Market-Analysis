import os
import csv
import requests
from dotenv import load_dotenv
from time import sleep

def get_news_data(params):
    """Returns NYT Article Headlines from their offical API

    Params:
    api-key: The API key
    begin_date: The oldest article publication date (yyyymmdd)
    q: The query string (to specify article category)
    """

    # Set the API URL
    api_url = "https://api.nytimes.com/svc/search/v2/articlesearch.json"

    # Make the API request and convert the response to JSON
    articles = requests.get(api_url, params=params).json()['response']

    # Exract headlines from the articles
    headlines = [article['abstract'] for article in articles['docs']]
    # headlines = [article['headline']['main'] for article in articles['docs']]
    
    return headlines

def write_to_csv(categorized_headlines):
    """Prepares dataset by writing Category and Headlines to a CSV file

    Params:
    categorized_headlines: Dictionary with category as key and list of headlines as value
    """

    with open("../Datasets/nyTimesData.csv", "a") as data_csv:
        csv_writer = csv.writer(data_csv)

        for category in categorized_headlines:
            for headline in categorized_headlines[category]:
                csv_writer.writerow([category, headline])

        data_csv.close()

def remove_duplicates(all_headlines, new_headlines):
    """Removes headlines from the new list which are already in the dataset

    params:
    all_headlines: Set of all headlines
    new_headlines: List of newly queried headlines
    """

    filtered_headlines = []
    for headline in new_headlines:
        if headline not in all_headlines:
            filtered_headlines.append(headline)

    return filtered_headlines

if __name__ == "__main__":

    # load the .env file
    load_dotenv()

    category_keywords = {
        "Crypto": ["bitcoin", "dogecoin", "crypto"],
        "Oil": ["crude oil", "oil prices"],
        "EVs": ["tesla motors", "electric vehicle", "EVs"],
        "Gaming": ["XBox", "Playstation", "Video Games"],
        "Tech": ["Apple", "Facebook", "Google", "Amazon"]
    }

    # API parameters
    params = {
        'api-key': os.getenv('API_KEY'),
        'begin_date': '20200101',
    }

    # Set of all headlines extracted (used to remove duplicates)
    all_headlines = set()

    # Dictionary of headlines from each category
    categorized_headlines = dict()

    for category in category_keywords:
        print("\nCategory: " + category)

        categorized_headlines[category] = []

        for keyword in category_keywords[category]:
            print("\tKeyword: " + keyword)
            
            # Set the keyword as query and get all headlines
            params['q'] = keyword
            headlines = get_news_data(params)

            # Add to the category's headline list
            categorized_headlines[category].extend(headlines)

        # Remove duplicate headlines within the category
        categorized_headlines[category] = list(set(categorized_headlines[category]))

        # Remove headlines that were already extracted by some other category
        categorized_headlines[category] = remove_duplicates(all_headlines, categorized_headlines[category])

        # Extend the list of all headlines with this list
        all_headlines.update(categorized_headlines[category])

        # Printing the count of headlines extracted for the current category
        print("Extracted", len(categorized_headlines[category]), "headlines")

        # Sleeping for 1 minute to avoid breaching API limits
        sleep(60)

    # Write extracted data to CSV
    write_to_csv(categorized_headlines)