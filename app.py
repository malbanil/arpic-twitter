#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct 17 19:58:03 2020

@author: albanilm
"""
import os
import tweepy
import mysql.connector
import json
import time
from datetime import date
#import sys

#Twitter credentials
credentials = {}
credentials['CONSUMER_KEY'] = os.environ['TWCONSUMER_KEY']
credentials['CONSUMER_SECRET'] = os.environ['TWCONSUMER_SECRET']
credentials['ACCESS_TOKEN'] = os.environ['TWACCESS_TOKEN']
credentials['ACCESS_SECRET'] = os.environ['TWACCESS_SECRET']

#RDS credentials
RDSHOST = "db-arpic.cjdm15sdlirv.us-east-1.rds.amazonaws.com"
RDSUSER = "adminarpic" 
RDSPASS = "adminarpic$2020"
RDSDB = "arpic"

FILE_TNAME = 'last_twiid.txt'
CRON_TIME = 900

def getLastTwiid(FILE_TNAME):
    file_read = open(FILE_TNAME,'r')
    last_id = int(file_read.read().strip())
    file_read.close()
    return last_id

def setLastTwiid(FILE_TNAME,last_id):
    file_write = open(FILE_TNAME,'w')
    file_write.write(str(last_id))
    file_write.close()
    return

def arpicInsertRdsTwitter(mydb,user,msg):
    now = date.today()
    rds_date = now.strftime("%Y-%m/%d %H:%M:%S")
    rds_cursor = mydb.cursor()
    query = ("INSERT INTO arpic.arpic_messages(id,date,source,card_user,message,status) VALUES (NULL,%s,'twitter',%s,%s,0)")
    rds_values = [rds_date,user,msg]
    rds_cursor.execute(query,rds_values)
    mydb.commit()
    return int(rds_cursor.rowcount)

def main():
    auth = tweepy.OAuthHandler(credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
    auth.set_access_token(credentials['ACCESS_TOKEN'], credentials['ACCESS_SECRET'])
    api = tweepy.API(auth)
    
    #RDS flow
    mydb = mysql.connector.connect(
      host=RDSHOST,
      user=RDSUSER,
      password=RDSPASS,
      database=RDSDB
    )
    cursor = mydb.cursor()
    
    #Get mentions
    last_twitter_id = getLastTwiid(FILE_TNAME)
    public_tweets = api.mentions_timeline(last_twitter_id, tweet_mode='extend')

    print(str(len(public_tweets)) + " number of statuses have been mentioned.") 
    #sys.exit('Debug')
    if public_tweets:
        for tweet in reversed(public_tweets):
            user_info ={   
              "name": tweet.user.name,   
              "id": tweet.user.id,   
              "account": tweet.user.screen_name,
              "followers_count": tweet.user.followers_count,
              "friends_count": tweet.user.friends_count
            }
            json_user = json.dumps(user_info, indent = 4) 
            url_msg = 'https://twitter.com/'+tweet.user.screen_name+'/status/'+str(tweet.user.id)
            full_msg = tweet.text+'/n/n **Responder:**'+url_msg
            #print(tweet.text)
            arpicInsertRdsTwitter(mydb,json_user ,full_msg)
            print('- Add twit '+str(tweet.id))
            setLastTwiid(FILE_TNAME,tweet.id)
    else:
        print("- Not new twits.") 
        
 


if __name__ == '__main__':
    while True:
        main()
        time.sleep(CRON_TIME)
        print("Scanning ARPIC Twitter messages V2...")


