# -*- coding: utf-8 -*-
"""
Created on Wed Sep 28 11:55:14 2021
Updated on Mon Sep 18 15:47:05 2022
@author: Andrés Segura-Tinoco
"""

# Import libraries
import csv
from datetime import datetime

import tweepy
import yaml
from pymongo import MongoClient
from requests.exceptions import ConnectionError, SSLError, Timeout

######################
### CORE FUNCTIONS ###
######################

# Util function - Extracts only the relevant fields of each tweet
def deserialize_tweet(id_str, tweet):
    message_bin = tweet.full_text.encode("utf-8")
    new_tweet = {
        "id": id_str,
        "user_name": tweet.user.screen_name,
        "created_at": tweet.created_at,
        "message": message_bin.decode(),
        "message_bin": message_bin,
        "lang": tweet.lang,
        "hashtags": [ht["text"] for ht in tweet.entities["hashtags"]],
        "user_mentions": [mt["screen_name"] for mt in tweet.entities["user_mentions"]],
        "retweet_count": tweet.retweet_count,
        "favorite_count": tweet.favorite_count,
        "retweeted": tweet.retweeted or message_bin.decode().startswith("RT "),
        "source": tweet.source.encode("utf-8"),
    }
    return new_tweet


# Util function - Read dict from yaml file
def get_dict_from_yaml(yaml_path: str) -> dict:
    result = dict()

    with open(yaml_path) as f:
        yaml_file = f.read()
        result = yaml.load(yaml_file, Loader=yaml.FullLoader)

    return result


# Twitter function - Read Twitter API authentication credentials
def get_twitter_auth():
    # Read twitter bot credentials
    yaml_path = "code/config/twt_credentials.yml"
    twt_login = get_dict_from_yaml(yaml_path)

    # Setup bot credentials
    consumer_key = twt_login["consumer_key"]
    consumer_secret = twt_login["consumer_secret"]
    access_token = twt_login["access_token"]
    access_token_secret = twt_login["access_token_secret"]

    # Authenticate to Twitter
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    return auth


# MongoDB function - Read MongoDB authentication credentials
def get_mongodb_auth():

    yaml_path = "code/config/mdb_credentials.yml"
    auth = get_dict_from_yaml(yaml_path)

    return auth


# MongoDB function - Get last tweet date
def get_tweets_date_since(mdb_login, max_date=False):
    date_since = datetime(2022, 9, 1)

    # Find out the max date from MongoDB
    if max_date:
        client = MongoClient(mdb_login["server"], mdb_login["port"])
        db = client[mdb_login["db"]]
        coll = db[mdb_login["collection"]]

        items = list(coll.find({}).sort([("created_at", -1)]).limit(1))
        if len(items) == 1:
            date_since = items[0]["created_at"]

    return date_since


# Twitter function - Fetch the list of tweets that tag a certain account
# Note: Twitter only allows access to a users most recent 3240 tweets with this method
def get_all_tweets_by_account(api, accounts, date_since) -> list:
    all_tweets = {}

    # Make initial request for most recent tweets (200 is the maximum allowed count)
    try:
        date_since_str = date_since.strftime("%Y-%m-%d")

        # Save most recent tweets
        for name in accounts:
            print(name)
            tweets = tweepy.Cursor(
                api.user_timeline,
                id=name,
                since_id=date_since_str,
                tweet_mode="extended",
            ).items()

            # Transform the tweepy tweets into a dict that contains the relevant fields of each tweet
            for tweet in tweets:
                key = tweet.id_str
                if key not in all_tweets:
                    all_tweets[key] = deserialize_tweet(key, tweet)

    except (tweepy.TwitterServerError) as e:
        print("Error 1:", e)

    except (Timeout, SSLError, ConnectionError) as e:
        print("Error 2:", e)

    return all_tweets


# Twitter function - Fetch the list of tweets that use certain hashtags
# Note: Twitter only allows access to a users most recent 3240 tweets with this method
def get_all_tweets_by_ht(api, hashtags, date_since) -> list:
    all_tweets = {}

    # Make initial request for most recent tweets (200 is the maximum allowed count)
    try:
        date_since_str = date_since.strftime("%Y-%m-%d")

        # Save most recent tweets
        for ht in hashtags:
            tweets = tweepy.Cursor(
                api.search_tweets, q=ht, since_id=date_since_str, tweet_mode="extended"
            ).items()

            # Transform the tweepy tweets into a dict that contains the relevant fields of each tweet
            for tweet in tweets:
                key = tweet.id_str
                if key not in all_tweets:
                    all_tweets[key] = deserialize_tweet(key, tweet)

    except (tweepy.TwitterServerError) as e:
        print("Error 1:", e)

    except (Timeout, SSLError, ConnectionError) as e:
        print("Error 2:", e)

    return all_tweets


# Util function - save tweet list to CSV file
def export_tweets_to_csv(csv_path: str, header: list, tweet_list: list) -> bool:
    result = False

    try:
        curr_row = None
        mode = "w"

        # using csv.writer method from CSV package
        with open(csv_path, mode, newline="", encoding="utf-8") as f:
            write = csv.writer(f)

            write.writerow(header)
            for row in tweet_list:
                curr_row = row
                hashtag_str = ",".join(row["hashtags"])
                mentions_str = ",".join(row["user_mentions"])
                row_data = [
                    row["id"],
                    row["created_at"],
                    row["user_name"],
                    row["lang"],
                    hashtag_str,
                    mentions_str,
                    row["retweet_count"],
                    row["favorite_count"],
                    row["retweeted"],
                    row["message"],
                ]
                write.writerow(row_data)
            result = True

    except Exception as e:
        print("Error", e, curr_row)

    return result


# Util function - Upsert documents into MongoDB
def mongodb_upsert_docs(mdb_login, doc_list):

    # Login
    client = MongoClient(mdb_login["server"], mdb_login["port"])
    db = client[mdb_login["db"]]
    coll = db[mdb_login["collection"]]
    total_docs = coll.count_documents({})
    print(f" - {coll.name} collection has {total_docs} total documents.")

    # Upsert documents
    for doc in doc_list:
        coll.update_one({"id": doc["id"]}, {"$set": doc}, upsert=True)


# Util function - Get all tweets from MongoDB
def get_all_tweets_from_mongodb(mdb_login):
    all_tweet_list = []

    # Login
    client = MongoClient(mdb_login["server"], mdb_login["port"])
    db = client[mdb_login["db"]]
    coll = db[mdb_login["collection"]]
    total_docs = coll.find()

    for doc in total_docs:
        tweet = {
            "id": doc["id"],
            "user_name": doc["user_name"],
            "created_at": doc["created_at"],
            "message": doc["message"],
            "lang": doc["lang"],
            "hashtags": doc["hashtags"],
            "user_mentions": doc["user_mentions"],
            "retweet_count": doc["retweet_count"],
            "favorite_count": doc["favorite_count"],
            "retweeted": doc["retweeted"],
            "source": doc["source"],
        }
        all_tweet_list.append(tweet)

    return all_tweet_list


def main():
    # 1. Create Twitter API bot
    auth = get_twitter_auth()
    api = tweepy.API(auth, wait_on_rate_limit=True)
    api.verify_credentials()
    print(" - Authentication OK")

    # 2. Show user account details
    tw_user_name = "@ACMRecSys"
    tw_hashtags = ["#RecSys2022", "#RecSys22"]
    user = api.get_user(screen_name=tw_user_name)
    print(" - User details:")
    print(f"   {user.name}")
    print(f"   {user.description}")
    print(f"   {user.location}")
    print(f"   {user.created_at}")

    # 3. Get max date
    mdb_login = get_mongodb_auth()
    date_since = get_tweets_date_since(mdb_login)
    print(f" - Downloading data since: {date_since}")

    # 4. Fetching tweet data
    tweet_set_1 = {}  # get_all_tweets_by_account(api, tw_user_name, date_since)
    tweet_set_2 = get_all_tweets_by_ht(api, tw_hashtags, date_since)
    tweet_list = {**tweet_set_1, **tweet_set_2}.values()
    print(len(tweet_set_1), len(tweet_set_2), len(tweet_list))

    # 5. Save tweets to MongoDB
    tweets_filepath = "data/tweets2022.csv"
    header = [
        "id",
        "created_at",
        "user_name",
        "lang",
        "hashtags",
        "user_mentions",
        "retweet_count",
        "favorite_count",
        "retweeted",
        "message",
    ]
    mongodb_upsert_docs(mdb_login, tweet_list)
    print(" - Tweets downloaded:", len(tweet_list))

    # 6. Export all tweets to CSV file
    all_tweet_list = get_all_tweets_from_mongodb(mdb_login)
    export_tweets_to_csv(tweets_filepath, header, all_tweet_list)
    print(" - Total tweets:", len(tweet_list))


#####################
### START PROGRAM ###
#####################
if __name__ == "__main__":
    print(">> START PROGRAM:", str(datetime.now()))
    main()
    print(">> END PROGRAM:", str(datetime.now()))
#####################
#### END PROGRAM ####
#####################
