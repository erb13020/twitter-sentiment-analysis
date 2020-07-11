import datetime as dt

import pandas as pd
import random

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

def detector(x):
    try:
        return detect(x)
    except:
        None


def scrape(y, m, d, query):
    begin_date = dt.date(y, m, 1)
    end_date = dt.date(y, m, d)

    tweets = query_tweets(query, begindate=begin_date, enddate=end_date, poolsize=d)

    df = pd.DataFrame(t.__dict__ for t in tweets)

    # filter for english tweets
    df['lang'] = df['text'].apply(lambda x: detector(x))
    df = df[df['lang'] == 'en']
    
    return df

def twitter_scraper(query_input, start_year, start_month, end_year, end_month):
    while True:
        
        print(
            "Now searching for tweets about " + query_input + " for the month of " + str(start_month) + " and the year of " + str(
                start_year))
        days = 0
    
        thirtyone_day_months = [1, 3, 5, 7, 8, 10, 12]
        thirty_day_months = [4, 6, 9, 11]
    
        if start_month in thirtyone_day_months:
            days = 31
        elif start_month in thirty_day_months:
            days = 30
        elif start_month == 2 and (start_year % 4 == 0):
            days = 29
        elif start_month == 2 and (start_year % 4 > 0):
            days = 28
    
        scrape(start_year, start_month, days, query_input)
    
        if start_month == 12:
            start_month = 1
            year = year + 1
        else:
            start_month = start_month + 1
    
        if ((start_month == end_month) and (start_year == end_year)):
            breakpoint()
            break

twitter_scraper('coronavirus', 2020, 2, 2020, 6)
# query_input = input("Enter your keyword: ")
# year = int(input("Enter Start Year: "))
# month = int(input("Enter Start Month: "))
# current_year = int(input("Enter Current Year: "))
# current_month = int(input("Enter Current Month: "))