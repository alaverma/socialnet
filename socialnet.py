#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec 28 16:09:52 2019

@author: andreslaverdemarin
"""

# Llireries necesraies per la recoleczió de dades
import tweepy
import GetOldTweets3 as got
import networkx as nx
# Importamos PyMongo per l'emmagatzematge
from pymongo import MongoClient
import pymongo

# Llibreries necesaries pel tractament dels tweets
import operator
import datetime
import pandas as pd
import json
import math

# Llibreries necesaries per trobar les paraules més rellevants
import re
from nltk.corpus import stopwords
from wordcloud import WordCloud, STOPWORDS
from stop_words import get_stop_words
import matplotlib.pyplot as plt


class tweetnet():
    
    # Diferents valors generics fets servir durant el programa
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
    
    # Busca els hashtags que és fiquin com a parametre
    def searchHashTag(self, hashTags):
        
        listt = []
        
        # Se pueden usar operadores binarios (más detalle en la documentación de la API)
        for tweet in tweepy.Cursor(self.api.search,q=(hashTags), count=100, tweet_mode= 'extended').items():
            listt.append(tweet)
            self.db.tweets.insert_one(tweet._json)
    
    # Crea el graf amb les dades preses a la funció anterior i el guarda amb
    # un fitxer .graphml
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
                    
        nx.write_graphml(DG, "./IndependenceTweet.graphml")
        

    # Neteja la BD eliminant els tweets repetits       
    def cleanDB(self):
        
       
        # Save data in dataframe
        df = pd.DataFrame(list(self.db.tweets.find()))
        
        # Delete tweets with duplicated information
        df = df.drop_duplicates(subset='id', keep='first')
        
        # Save in pkl file 
        df.to_pickle("df_tweets.pkl", compression='zip')
        
        # Save in mongodb document 
        self.db3.tweets_clean_final.insert_many(df.to_dict(orient='records'))
    
    # Selecciona els id per comunitat a la que pertanyen i els guarda en un
    # diccionari
    def tweetinfoCommunity(self):
        
        df = pd.read_pickle("df_tweets.pkl", compression='zip')
        
        for i in df.columns:
            print(i)
        
        #df["full_text"]
        
        df_gephi = pd.read_csv("independence_graf_5_core.csv")
        
        modularity_dict = {}
        for mod in range(0,8):
            g = df_gephi[df_gephi["modularity_class"] == mod]["Id"].tolist()
            modularity_dict[mod] = g
            
        #for i in modularity_dict:
        text_dict = {}
        for mod in modularity_dict:
            text = []
            for user_mod in modularity_dict[mod]:
                b = 0
                for user in df["user"]:
                    if user['screen_name'] == user_mod:
                        text.append(df.iloc[b]["full_text"])
                    b = b + 1
                    
            text_dict[mod] = " ".join(text)
        
        return text_dict
    
    # Busca i neteja el text de cadascun dels usuaris de tweeter, per comunitats
    def cleanText(self, dictiory):
        # This part of code clean text obtained before
        for group in dictiory.keys():
    
            # Delete all mentions
            final = re.sub(r'@[\w-]+', '', dictiory[group])

            # Delete all hashtags
            final = re.sub(r'#[\w-]+', '', final)

            # Delete all hashtags
            final = re.sub(r'RT', '', final)

            # Delete all hashtags
            final = re.sub(r'&[\w-]+', '', final)

            # Delete all emails
            final = re.sub(r'[https://]+/[\w\.\/\w-]+', '', final)

            # Delete all puntuation marks except '
            result = re.sub(r"[^\w\d\s]",'',final)

            # Delete more thant one space 
            result = re.sub(r"\s+"," ", result)
    
            # Delete one letter, nomber or simbol that stay alone in a text
            result = re.sub(r'(?:^| )\w(?:$| )', ' ', result).strip()

            dictiory[group] = result.lower()
    
            # Delete one letter, nomber or simbol that stay alone in a text
            result = re.sub(r'(?:^| )\w(?:$| )', ' ', result).strip()
        
        oder_split_words = ["que", "del", "lo"]
        # This part of the code count frequence and delete stopwords    
        for group in dictiory.keys():
            cleantokens = dictiory[group].split()[:]
            for token in dictiory[group].split():
                if token in get_stop_words('catalan') or token in stopwords.words("spanish") or token in stopwords.words("english"):
                    cleantokens.remove(token)
            dictiory[group] = " ".join(cleantokens)
            
        # Visualitzem el resultat
        plt.figure(num=None, figsize=(22, 20))

        for group in dictiory.keys():
           wordcloud = WordCloud(max_font_size=50, max_words=50, 
                                 background_color="white").generate(dictiory[group])
    
           plt.figure(num=None, figsize=(22, 20))
           #plt.set_title("Group: {}".format(group))
           plt.imshow(wordcloud, interpolation='bilinear')
           plt.axis("off")
           plt.savefig('Text_group_{}.png'.format(group))
        
# Execució de codi segons el que necesitem        
def main():
    
    tw = tweetnet()
    
    hashTags = "#SpainIsAFascistState OR #SentenciaProces OR #Independencia OR #CataluñaEsEspaña OR #recuperemelseny OR #EstamosporTI OR #LlibertatPresosPoliticsiexiliats OR #Llibertatpresospolitics"
    
    
    tw.searchHashTag(hashTags)
    
    tw.cleanDB()
    
    tw.createGraph()
    
    comunity_text = tw.tweetinfoCommunity()
    
    tw.cleanText(comunity_text)
    
if __name__ == "__main__":
    main()