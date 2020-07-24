import datetime as dt
import logging
import random
import time

import pandas as pd
import twitterscraper
from langdetect import detect
from twitterscraper import query_tweets

logger = logging.getLogger('twitterscraper')
logger.setLevel(logging.WARNING)

HEADERS_LIST = [
    'Mozilla/5.0 (Windows; U; Windows NT 6.1; x64; fr; rv:1.9.2.13) Gecko/20101203 Firebird/3.6.13',
    'Mozilla/5.0 (compatible, MSIE 11, Windows NT 6.3; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows; U; Windows NT 6.1; rv:2.2) Gecko/20110201',
    'Opera/9.80 (X11; Linux i686; Ubuntu/14.10) Presto/2.12.388 Version/12.16',
    'Mozilla/5.0 (Windows NT 5.2; RW; rv:7.0a1) Gecko/20091211 SeaMonkey/9.23a1pre'
]

twitterscraper.query.HEADER = {'User-Agent': random.choice(HEADERS_LIST), 'X-Requested-With': 'XMLHttpRequest'}


def detector(s):
    '''
    Helper function for detecting language of a given string s using the langdetect module.

            Parameters:
                    s (str): A string.

            Returns:
                    detect(s) (str): string of length 2 indicating language of s.
    '''
    try:
        return detect(s)
    except:
        None


def scrape(d, m, y, query):
    '''
    Returns a dataframe containing all tweets and metadata for a query in a given month and filters for only English tweets.

            Parameters:
                    d (int): An integer representing the day.
                    m (int): A 2 digit integer representing the month.
                    y (int): A 4 digit integer representing the year.
                    query (str): The twitter query.

            Returns:
                    df (DataFrame): DataFrame containing all tweets and metadata for a query.
    '''
    begin_date = dt.date(y, m, d)
    end_date = begin_date + dt.timedelta(days=1)

    tweets = query_tweets(query, begindate=begin_date, enddate=end_date, poolsize=60)

    df = pd.DataFrame(t.__dict__ for t in tweets)

    df['lang'] = df['text'].apply(lambda x: detector(x))
    df = df[df['lang'] == 'en']

    print('Scraped ' + str(len(df)) + ' tweets for ' + str(d) + '/' + str(m) + '/' + str(y))

    return df


def twitter_scraper(query, start_day, start_month, start_year, end_day, end_month, end_year):
    '''
    Iterate scraping for all tweets about query from start_month,start_year to end_month,end_year

            Parameters:
                    query (str): A string representing the query.
                    start_day (int): An integer representing the starting day.
                    start_month (int): An integer representing the starting month.
                    start_year (int): An integer representing the starting year.
                    end_day (int): An integer representing the starting year.
                    end_month (int): An integer representing the ending month.
                    end_year (int): An integer representing the ending year.

            Returns:
                    df_result (DataFrame): A DataFrame containing all tweets, hashtags, likes, retweets, and replies for a query from starting to end dates.
    '''

    start_time = time.time()
    d = start_day
    m = start_month
    y = start_year
    df_result = pd.DataFrame()

    while True:

        print("Scraping tweets for " + str(d) + "/" + str(m) + "/" + str(y))
        df = scrape(d, m, y, query)
        df_result = df_result.append(df, ignore_index=True)

        if ((d == 30) and m in [4, 5, 6, 9, 11]):
            d = 1
            m = m + 1
        elif ((d == 31) and m in [1, 3, 7, 8, 10]):
            d = 1
            m = m + 1
        elif ((d == 31) and (m == 12)):
            d = 1
            m = 1
            y = y + 1
        elif ((d == 28) and (m == 2) and (y % 4 > 0)):
            d = 1
            m = m + 1
        elif ((d == 29) and (m == 2) and (y % 4 == 0)):
            d = 1
            m = m + 1
        else:
            d = d + 1

        if (d == end_day) and (m == end_month) and (y == end_year):
            df_result = df_result.drop_duplicates(subset='tweet_id', keep='first')
            df_result.reset_index(drop=True, inplace=True)

            df_result = df_result[['user_id', 'tweet_id', 'text', 'timestamp', 'hashtags', 'likes', 'retweets', 'replies', 'parent_tweet_id', 'reply_to_users']]

            print('Scraped ' + str(len(df_result)) + ' tweets about ' + query + ' from '
                  + str(start_month) + '/' + str(start_year) + ' to ' + str(end_month) + '/' + str(end_year)
                  + ' in ' + str(int(time.time() - start_time)) + ' seconds.')

            df_result.to_csv('tweets.gz', encoding='utf-8', index=False)

            return df_result

twitter_scraper('tesla', 28, 2, 2020, 29, 2, 2020)
