# -*- coding: utf-8 -*-
"""
Created on Wed Sep 28 11:55:14 2021

@author: ansegura
"""

# Import libraries
import csv
import yaml
import tweepy
from requests.exceptions import Timeout, SSLError, ConnectionError
from requests.packages.urllib3.exceptions import ReadTimeoutError, ProtocolError
from datetime import datetime
from pymongo import MongoClient

######################
### CORE FUNCTIONS ###
######################

# Util function - Read dict from yaml file
def get_dict_from_yaml(yaml_path:str) -> dict:
    result = dict()
    
    with open(yaml_path) as f:
        yaml_file = f.read()
        result = yaml.load(yaml_file, Loader=yaml.FullLoader)
    
    return result

# Twitter function - Read Twitter API authentication credentials
def get_twitter_auth():
    # Read twitter bot credentials
    yaml_path = '../code/config/twt_credentials.yml'
    twt_login = get_dict_from_yaml(yaml_path)
    
    # Setup bot credentials
    consumer_key = twt_login['consumer_key']
    consumer_secret = twt_login['consumer_secret']
    access_token = twt_login['access_token']
    access_token_secret = twt_login['access_token_secret']
    
    # Authenticate to Twitter
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    
    return auth

# MongoDB function - Read MongoDB authentication credentials
def get_mongodb_auth():
    
    yaml_path = 'config/mdb_credentials.yml'
    auth = get_dict_from_yaml(yaml_path)
    
    return auth

# MongoDB function - Get last tweet date
def get_tweets_since_date(mdb_login, max_date=False):
    since_date = datetime(2021,9, 1)

    # Find out the max date from MongoDB    
    if max_date:
        client = MongoClient(mdb_login['server'], mdb_login['port'])
        db = client[mdb_login['db']]
        coll = db[mdb_login['collection']]
        
        items = list(coll.find({}).sort([("created_at", -1)]).limit(1))
        if len(items) == 1:
            since_date = items[0]["created_at"]
    
    return since_date

# Twitter function - Fetch tweets list from a specific user
# Note: Twitter only allows access to a users most recent 3240 tweets with this method
def get_all_tweets_by_ht(api, hashtags, since_date) -> list:
    all_tweets = {}
    
    # Make initial request for most recent tweets (200 is the maximum allowed count)
    try:
        since_date_str = since_date.strftime('%Y-%m-%d')        
        
        # Save most recent tweets        
        for ht in hashtags:
            for tweet in tweepy.Cursor(api.search, q=ht, since=since_date_str).items():
                if tweet.id_str not in all_tweets:
                    all_tweets[tweet.id_str] = tweet    
    
    except (tweepy.TweepError) as e:
        print('Error 1:', e)
    
    except (Timeout, SSLError, ConnectionError, ReadTimeoutError, ProtocolError) as e:    
        print('Error 2:', e)
    
    # Transform the tweepy tweets into an array that contains the relevant fields of each tweet
    tweet_list = []
    for id_str, tweet in all_tweets.items():
        message_bin = tweet.text.encode('utf-8')
        new_tweet = {
            'id': id_str,
            'user_name': tweet.user.screen_name,
            'created_at': tweet.created_at,
            'message': message_bin.decode(),
            'message_bin': message_bin,
            'lang': tweet.lang,
            'hashtags': [ht['text'] for ht in tweet.entities['hashtags']],
            'user_mentions': [mt['screen_name'] for mt in tweet.entities['user_mentions']],
            'retweet_count': tweet.retweet_count,
            'favorite_count': tweet.favorite_count,
            'retweeted': tweet.retweeted or message_bin.decode().startswith("RT "),
            'source': tweet.source.encode('utf-8')
        }
        tweet_list.append(new_tweet)
    
    return tweet_list

# Util function - save tweet list to CSV file
def export_tweets_to_csv(csv_path:str, header:list, tweet_list:list) -> bool:
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
                hashtag_str = ",".join(row['hashtags'])
                mentions_str = ",".join(row['user_mentions'])
                row_data = [row['id'], row['created_at'], row['user_name'], row['lang'], hashtag_str, mentions_str, 
                            row['retweet_count'], row['favorite_count'], row['retweeted'], row['message']]
                write.writerow(row_data)
            result = True
        
    except Exception as e:
        print("Error", e, curr_row)
    
    return result

# Util function - Upsert documents into MongoDB
def mongodb_upsert_docs(mdb_login, doc_list):
    
    # Login
    client = MongoClient(mdb_login['server'], mdb_login['port'])
    db = client[mdb_login['db']]
    coll = db[mdb_login['collection']]
    total_docs = coll.count_documents({})
    print ('', coll.name, "has", total_docs, "total documents.")
    
    # Upsert documents
    for doc in doc_list:
        coll.update_one(
            {"id" : doc['id']},
            {"$set": doc},
            upsert=True
        )

# Util function - Get all tweets from MongoDB
def get_all_tweets_from_mongodb(mdb_login):
    all_tweet_list = []
    
     # Login
    client = MongoClient(mdb_login['server'], mdb_login['port'])
    db = client[mdb_login['db']]
    coll = db[mdb_login['collection']]
    total_docs = coll.find()
    
    for doc in total_docs:
        tweet = {'id': doc['id'],
            'user_name': doc['user_name'],
            'created_at': doc['created_at'],
            'message': doc['message'],
            'lang': doc['lang'],
            'hashtags': doc['hashtags'],
            'user_mentions': doc['user_mentions'],
            'retweet_count': doc['retweet_count'],
            'favorite_count': doc['favorite_count'],
            'retweeted': doc['retweeted'],
            'source': doc['source']}
        all_tweet_list.append(tweet)
    
    return all_tweet_list

#####################
### START PROGRAM ###
#####################
if __name__ == "__main__":
    print('>> START PROGRAM:', str(datetime.now()))
    
    # 1. Create Twitter API bot
    auth = get_twitter_auth()
    api = tweepy.API(auth, wait_on_rate_limit=True)
    api.verify_credentials()
    print(">> Authentication OK")
    
    # 2. Show user account details
    tw_user_name = "@ACMRecSys"
    user = api.get_user(screen_name=tw_user_name)
    print(">> User details:")
    print('  ', user.name)
    print('  ', user.description)
    print('  ', user.location)
    print('  ', user.created_at)
    
    # 3. Get max date
    mdb_login = get_mongodb_auth()
    since_date = get_tweets_since_date(mdb_login)
    
    # 4. Fetching tweet list from a specific user
    hashtags = ["#RecSys2021", "#RecSys21"]
    tweet_list = get_all_tweets_by_ht(api, hashtags, since_date)
    
    # 5. Save tweets to MongoDB
    yaml_path = '../data/tweets.csv'
    header = ['id', 'created_at', 'user_name', 'lang', 'hashtags', 'user_mentions', 'retweet_count', 'favorite_count', 'retweeted', 'message']
    mongodb_upsert_docs(mdb_login, tweet_list)
    print('>> Tweets downloaded:', len(tweet_list))
    
    # 6. Export all tweets to CSV file
    all_tweet_list = get_all_tweets_from_mongodb(mdb_login)
    export_tweets_to_csv(yaml_path, header, all_tweet_list)
    print('>> Total tweets:', len(tweet_list))
    
    print(">> END PROGRAM:", str(datetime.now()))
#####################
#### END PROGRAM ####
#####################
