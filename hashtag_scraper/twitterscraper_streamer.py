import datetime as dt
import random

import pandas as pd
import twitterscraper
from langdetect import detect
from twitterscraper import query_tweets

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


def scrape(y, m, query):
    '''
    Returns a dataframe containing all tweets and metadata for a query in a given month and filters for only English tweets.

            Parameters:
                    y (int): A 4 digit integer representing the year.
                    m (int): A 2 digit integer representing the month.
                    query (str): The twitter query.

            Returns:
                    df (DataFrame): DataFrame containing all tweets and metadata for a query.
    '''
    d = __calculate_days(m, y)
    begin_date = dt.date(y, m, 1)
    end_date = dt.date(y, m, d)

    tweets = query_tweets(query, begindate=begin_date, enddate=end_date, poolsize=d)

    df = pd.DataFrame(t.__dict__ for t in tweets)

    df['lang'] = df['text'].apply(lambda x: detector(x))
    df = df[df['lang'] == 'en']
    df.reset_index(drop=True, inplace=True)

    return df


def __calculate_days(m, y):
    '''
    Private helper function for calculating the days in a month while accounting for leap years.

            Parameters:
                    m (int): A 4 digit integer representing the year.
                    y (int): A 2 digit integer representing the month.

            Returns:
                    d (int): An int representing the days in a given month.
    '''

    thirtyone_day_months = [1, 3, 5, 7, 8, 10, 12]
    thirty_day_months = [4, 6, 9, 11]

    if m in thirtyone_day_months:
        d = 31
    elif m in thirty_day_months:
        d = 30
    elif m == 2 and (y % 4 == 0):
        d = 29
    elif m == 2 and (y % 4 > 0):
        d = 28

    return d


def twitter_scraper(query, start_year, start_month, end_year, end_month):
    '''
    Iterate scraping for all tweets about query from start_month,start_year to end_month,end_year

            Parameters:
                    query (str): A string representing the query.
                    start_year (int): An integer representing the starting year.
                    start_month (int): An integer representing the starting month.
                    end_year(int): An integer representing the starting year.
                    end_month(int): An integer representing the ending month.

            Returns:
                    df_result (DataFrame): A DataFrame containing all tweets, hashtags, likes, retweets, and replies for a query from starting to end dates.
    '''
    df_result = pd.DataFrame()
    while True:

        df = scrape(start_year, start_month, query)

        df_result = df_result.append(df, ignore_index=True)

        df.reset_index(drop=True, inplace=True)

        if start_month == 12:
            start_month = 1
            start_year = start_year + 1
        else:
            start_month = start_month + 1

        if (start_month == end_month) and (start_year == end_year):
            df_result = df_result.drop_duplicates(subset='tweet_id', keep='first')
            df_result = df_result[['text', 'timestamp', 'hashtags', 'likes', 'retweets', 'replies']]

            return df_result
