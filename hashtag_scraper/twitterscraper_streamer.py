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
    #search for every tweet containing query for the month
    #I would have liked to just scrape all the tweets from the start_date to end_date but
    #It starts breaking when you try to scrape dates for more than 31 dates
    #So we just recrusively run this function until we've scraped tweets for every month
    #inbetween the start_date and end_date to over ride this restriction

    begin_date = dt.date(y, m, 1)
    end_date = dt.date(y, m, d)

    tweets = query_tweets(query, begindate=begin_date, enddate=end_date, poolsize=d)

    df = pd.DataFrame(t.__dict__ for t in tweets) #put all the tweets into a dataframe

    # filter for english tweets
    df['lang'] = df['text'].apply(lambda x: detector(x)) #create a new column 'lang', pass df['text'] to the langdetect detector function
    df = df[df['lang'] == 'en'] #Only look for tweets in the English language

    # This is where I would save the tweets for that month to a .csv file

    #     file_name = query + '_tweets_' + str(y) + '_' + str(m) +'.csv'
    #     path = '~/PycharmProjects/bible_tweets/raw_tweets/' + file_name
    #     df_christianity.to_csv(path, encoding='utf-8')

    # I would then later merge all the tweets for each month to one .csv
    
    return df #for now just return the dataframe of the tweets for the month

def twitter_scraper(query_input, start_year, start_month, end_year, end_month):
    while True:

        #see comment under the scrape(y,m,d,query) function as to why we can only look at tweets
        #in increments of months

        print(
            "Now searching for tweets about " + query_input + " for the month of " + str(start_month) + " and the year of " + str(
                start_year))
        days = 0

        #calculate how many days are are in the current month
        #This could definetly be improved

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

        #We pass this to our helper function for scraping
        scrape(start_year, start_month, days, query_input)


        #If we're going from December to January - add 1 to year and set current month to 1 (january)
        if start_month == 12:
            start_month = 1
            start_year = start_year + 1
        else:
            # otherwise move our date window up a month
            start_month = start_month + 1

        #If we're past the end_date, we can end the program
        if ((start_month == end_month) and (start_year == end_year)):
            break

#Let's scrape all tweets about the coronavirus from Feb 2020 to June 2020
twitter_scraper('coronavirus', 2020, 2, 2020, 6)