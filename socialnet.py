#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec 28 16:09:52 2019

@author: andreslaverdemarin
"""

import tweepy
import GetOldTweets3 as got
import networkx as nx
# Importamos PyMongo
from pymongo import MongoClient
import pymongo

import operator
import datetime

import pandas as pd
import json
import math


class tweetnet():
    
    def __init__(self):
        
        # Definim les claus d'acces a l'API
        consumer_key = 'oXv0jZW8HaTyXftkNmFtE4GOf'
        consumer_secret = 'EwGQDKFnGiTmWFH9Gd0hCm7urN0akHkfCOzPVc5CgoOycfaD0z'
        access_token = '853496815-3TtGcDxXTLJiulBFs8rAfftycVjOHdzsXhxaNQiE'
        access_token_secret = 'CllxSnt2RgsXiiGn4WLAGKu4LHLzHfymyo88zVzrziupS'
        
        # Preparem l'autenticació
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        
        # Creamos una conexión con la BBDD
        client = MongoClient()
           
        # Usamos una base de datos llamada test
        self.db = client.test
        
        # Usamos una base de datos llamada test
        self.db2 = client.backup
        
        # Usamos una base de datos llamada test
        self.db3 = client.final
        
        # Definim l'API que és la que farem servir per fer grides
        self.api = tweepy.API(auth)
        
    def searchHashTag(self, hashTags):
        
        listt = []
        
        # Se pueden usar operadores binarios (más detalle en la documentación de la API)
        for tweet in tweepy.Cursor(self.api.search,q=(hashTags), count=100, tweet_mode= 'extended').items():
            listt.append(tweet)
            self.db.tweets.insert_one(tweet._json)
        
    def createGraph(self):
        
        DG = nx.DiGraph()
        
        for tweet in self.db3.tweets_clean_final.find():
            
            # Check if is retweet
            if type(tweet['retweeted_status']) == dict:
                # Create edge or increase weight if exist edge
                if tweet['retweeted_status']['user']['screen_name'] == None:
                    print(tweet['retweeted_status'])
                A = tweet['user']['screen_name']
                B = tweet['retweeted_status']['user']['screen_name']
                if DG.has_edge(A, B) == True:
                    DG[A][B]['weight'] = DG[A][B]['weight'] + 1
                else:
                    DG.add_edge(A, B, weight=1)
                check = True
            # Check if is quote 
            if tweet['quoted_status'] == dict:
                # Create edge or increase weight if exist edge
                if tweet['quoted_status']['user']['screen_name'] == None:
                    print(tweet['quoted_status'])
                A = tweet['user']['screen_name']
                B = tweet['quoted_status']['user']['screen_name']
                if DG.has_edge(A, B) == True:
                    DG[A][B]['weight'] = DG[A][B]['weight'] + 1
                else:
                    DG.add_edge(A, B, weight=1)
                check = True
            # Check if tweet is reply   
            if math.isnan(tweet['in_reply_to_status_id']) is False:
                # Create edge or increase weight if exist edge
                A = tweet['user']['screen_name']
                B = tweet['in_reply_to_screen_name']
                if DG.has_edge(A, B) == True:
                    DG[A][B]['weight'] = DG[A][B]['weight'] + 1
                else:
                    DG.add_edge(A, B, weight=1)
                check = True
            # Check if tweet is isolated    
            if check == False:
                # If not exist node it is added at isolated node
                A = tweet['user']['screen_name']
                if A in DG:
                    continue
                else:
                    DG.add_node(A)
                    
        print(len(DG.nodes), DG.size())
        
        nx.write_graphml(DG, "./IndependenceTweet.graphml")
        
        indegree ={} 
        for node in list(DG.nodes):
            indegree[node] = DG.in_degree(node, weight='weight')

        degree = sorted(indegree.items(), key=operator.itemgetter(1), reverse=True)

        print(degree[:20])
        
    def delete(self):
        lis = []
        
        for tweet in self.db.tweets.find():
            a = tweet['created_at'].split(" ")
            datet = a[1]+" "+a[2]+" "+a[-1]
            date_time_obj = datetime.datetime.strptime(datet, '%b %d %Y')
            
            date_time_check = datetime.datetime.strptime("Dec 26 2019", '%b %d %Y')
            
            
            
            if tweet["id"] == 1211245992030474240:
                print(tweet)
                print("\n")
                lis.append(tweet)
                
        print(len(lis))
        for i in lis[0]:
            print(i)
        
    def cleanDB(self):
        
       
        # Save data in dataframe
        df = pd.DataFrame(list(self.db.tweets.find()))
        
        # Delete tweets with duplicated information
        df = df.drop_duplicates(subset='id', keep='first')
        
        # Save in pkl file 
        df.to_pickle("df_tweets.pkl", compression='zip')
        
        # Save in mongodb document 
        self.db3.tweets_clean_final.insert_many(df.to_dict(orient='records'))
        
        
def main():
    
    tw = tweetnet()
    
    hashTags = "#SpainIsAFascistState OR #SentenciaProces OR #Independencia OR #CataluñaEsEspaña OR #recuperemelseny OR #EstamosporTI OR #LlibertatPresosPoliticsiexiliats OR #Llibertatpresospolitics"
    
    #tw.searchHashTag(hashTags)
    
    tw.cleanDB()
    
    tw.createGraph()
    #tw.delete()
    
if __name__ == "__main__":
    main()