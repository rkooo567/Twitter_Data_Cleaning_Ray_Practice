"""
Data cleaning includes
Write code to drop tweets with pics and remove links from tweets
Write code to drop tweets not written in english
Write code to convert emojis to text, and later on, text to emojis
"""
import os
import json
import re
import emoji
import ray
import time

# Helper======================================================================================
def get_fields_of_dict(dic):
    """
    Twitter dict contains

    created_at
    id
    id_str
    text
    source
    truncated
    in_reply_to_status_id
    in_reply_to_status_id_str
    in_reply_to_user_id
    in_reply_to_user_id_str
    in_reply_to_screen_name
    user
    geo
    coordinates
    place
    contributors
    retweeted_status
    is_quote_status
    retweet_count
    favorite_count
    entities
    favorited
    retweeted
    filter_level
    lang
    timestamp_ms
    """
    for key in dic:
        print(key)

def get_twitter_data_path():
    # This code is for my program. It should be changed in the actual repo
    current_dir_list = os.path.dirname(os.path.realpath(__file__)).split('/')
    current_dir_list.pop()
    current_dir_list.append('data/twitter_data')
    twitter_data_path = '/'.join(current_dir_list)
    return twitter_data_path

def read_tweet_json_file():
    # Most of part at main is for a test purpose. Don't include them directly to the actual repo
    twitter_data_path = get_twitter_data_path()

    tweets = []   
    # Read json
    tweet_json_file = open("{}/vote_tweets.json".format(twitter_data_path), 'r')
    for line in tweet_json_file:
        try:
            tweet = json.loads(line)
            tweets.append(tweet)

        except(ValueError, KeyError, TypeError):
            # throw away the json data that has format errors
            #print("JSON format error. This tweet is aborted. This means there are some json data that causes errors")
            pass

    tweet_json_file.close()
    return tweets

# Core Fucntions====================================================================================================
def char_is_emoji(character):
    return character in emoji.UNICODE_EMOJI

def text_has_emoji(text):
    for character in text:
        if character in emoji.UNICODE_EMOJI:
            return True
    return False

def clean_tweet_dict(tweet):
    """
    Clean the dictionary in place. Note that language filter is not done here.
    """
    # remove links
    tweet['text'] = re.sub(r"http\S+", "", tweet['text'])
    
    # remove emojis
    tweet['text'] = emoji.demojize(tweet['text'])

    # remove pics & urls
    if "urls" in tweet["entities"]:
        tweet["entities"].pop("urls")

def twitter_pipelining(tweets):
    cleaned_result = []

    for tweet in tweets:
        if tweet['lang'] == 'en' and "media" not in tweet["entities"]:
            clean_tweet_dict(tweet)
            cleaned_result.append(tweet)
    
    return cleaned_result

def twitter_pipelining_with_ray(tweets):
    cleaned_result = []
    cleaned_result = ray.get([clean_tweet_dict_with_ray.remote(tweet) for tweet in tweets])
    
    return cleaned_result

@ray.remote
def clean_tweet_dict_with_ray(tweet):
    """
    Clean the dictionary in place with Ray.
    """
    if tweet['lang'] != 'en' or "media" in tweet["entities"]:
        return {}
    
    # remove links
    tweet['text'] = re.sub(r"http\S+", "", tweet['text'])
    
    # remove emojis
    tweet['text'] = emoji.demojize(tweet['text'])

    # remove pics & urls
    if "urls" in tweet["entities"]:
        tweet["entities"].pop("urls")
    
    return tweet

# Main=============================================================================================================================
def performance_test():
    NUM_OF_COPIES = 100

    # Read json file
    tweets = read_tweet_json_file()
    # increase num of tweets
    tweets = tweets * NUM_OF_COPIES
    print('num of tweets: {}'.format(len(tweets)))

    # Performance test without ray
    start = time.time()
    result = twitter_pipelining(tweets)
    end = time.time()
    print('performance without ray: {}'.format(end - start))

    # Read json file
    tweets = read_tweet_json_file()
    # increase num of tweets
    tweets = tweets * NUM_OF_COPIES
    print('num of tweets: {}'.format(len(tweets)))

    # Performance test with Ray
    start = time.time()
    result = twitter_pipelining_with_ray(tweets)
    end = time.time()
    print('Performance with ray: {}'.format(end - start))




