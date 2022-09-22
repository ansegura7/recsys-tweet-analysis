# Import libraries
from collections import Counter
from datetime import datetime

import pandas as pd

import tweets_downloader as lib


def get_hashtags_freq(df):
    ht_tokens = ",".join(str(v).lower() for v in df["hashtags"])
    ht_tokens = (
        ht_tokens.replace("[", "").replace("]", "").replace("'", "").replace(" ", "")
    )
    ht_list = [token for token in ht_tokens.split(",") if token != ""]
    ht_freq = dict(Counter(ht_list).most_common())

    return ht_freq


def get_bigrams_freq(df):
    return {}


def create_race_data(df, method):
    data = pd.DataFrame()

    # Min and max dates
    min_date = min(df["created_at"])
    max_date = max(df["created_at"])
    print(f"Min date: {min_date}, and max date: {max_date}")

    # While lopp
    while min_date <= max_date:
        min_date = min_date + pd.DateOffset(hours=12)
        col_name = str(min_date)
        curr_df = df[df["created_at"] < min_date]

        if method == "hashtags":
            freq_data = get_hashtags_freq(curr_df)

        elif method == "bigrams":
            freq_data = get_bigrams_freq(curr_df)
        else:
            freq_data = {}

        new_col = pd.DataFrame.from_dict(freq_data, orient="index", columns=[col_name])
        data = pd.concat([data, new_col], axis=1)
        print(
            f"Total data: {len(data)}, current date: {min_date}, total rows: {len(curr_df)}, total columns: {len(data.columns)}, total tokens: {len(freq_data)}"
        )

    # Complete NaN with 0 and return data
    data = data.fillna(0)
    return data


# Program start point
def main():

    # Get all tweets from MongoDB
    mdb_login = lib.get_mongodb_auth()
    all_tweet_list = lib.get_all_tweets_from_mongodb(mdb_login)

    if len(all_tweet_list):
        df = pd.DataFrame.from_records(all_tweet_list)
        df["created_at"] = pd.to_datetime(df["created_at"], format="%d%b%Y:%H:%M:%S.%f")

        # Create race data
        data_ht = create_race_data(df, "hashtags")
        data_bg = create_race_data(df, "bigrams")

        # Export hashtag and bigrams data to CSV files
        data_ht.to_csv("data/ht_race_data.csv")
        data_bg.to_csv("data/bg_race_data.csv")


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
