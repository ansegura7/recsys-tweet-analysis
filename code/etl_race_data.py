# Import libraries
import re
from collections import Counter
from datetime import datetime

import pandas as pd

import tweets_downloader as lib

# Global NLP variables
punt_marks = [
    "\n",
    '"',
    "\\",
    "/",
    "¡",
    "!",
    "¿",
    "?",
    ".",
    ",",
    ";",
    ":",
    "_",
    "-",
    "#",
    "$",
    "%",
    "&",
    "(",
    ")",
    "'",
]
rx = "[" + re.escape("".join(punt_marks)) + "]"


def get_stopwords(lang: str) -> set:
    stopwords = []

    filename = "data/stopwords/" + lang + ".txt"
    with open(filename) as file:
        lines = file.readlines()
        stopwords = [line.rstrip() for line in lines]

    return set(stopwords)


# Util function - Clean tweet text
def dq_clean_text(text):
    clean_text = text.lower()
    clean_text = re.sub(rx, " ", clean_text)
    clean_text = re.sub(r"\.+", " ", clean_text)
    clean_text = re.sub(r"\s+", " ", clean_text)
    return clean_text


def get_hashtags_freq(col_data):
    ht_tokens = ",".join(str(v).lower() for v in col_data)
    ht_tokens = (
        ht_tokens.replace("[", "").replace("]", "").replace("'", "").replace(" ", "")
    )
    ht_list = [token for token in ht_tokens.split(",") if token != ""]

    # Parsing and returning data
    ht_freq = dict(Counter(ht_list))
    return ht_freq


def get_bigrams_freq(col_data):

    # Calculate most common bigrams and reconstruct full text with used words
    bigram_list = Counter()
    new_clean_text = ""

    # Get Spanish stopwords
    stopwords_sp = get_stopwords("spanish")
    stopwords_en = get_stopwords("english") | set({"http", "https"})

    # Create list of words
    for tweet_text in col_data.tolist():
        clean_text = dq_clean_text(tweet_text)
        tokens = clean_text.split(" ")
        bigram = ""
        last_word = ""

        for word in tokens:
            if (
                (word not in stopwords_sp)
                and (word not in stopwords_en)
                and (len(word) > 2)
                and (word[0] != "@")
            ):
                # Reconstructing the clean text (without stop-words)
                new_clean_text += " " + word

                # Add bigrams-freq to Dataframe
                if last_word != "":
                    bigram = last_word + "-" + word
                    bigram_list[bigram] += 1

                last_word = word

    # Parsing and returning data
    bg_freq = dict(bigram_list)
    return bg_freq


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
            freq_data = get_hashtags_freq(curr_df["hashtags"])

        elif method == "bigrams":
            freq_data = get_bigrams_freq(curr_df["message"])
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

        # Create and save hashtags race data
        print(" - Create and save hashtags race data:")
        data_ht = create_race_data(df, "hashtags")
        data_ht.to_csv("data/ht_race_data.csv")

        # Create and save bigrams race data
        print(" - Create and save bigrams race data:")
        data_bg = create_race_data(df, "bigrams")
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
