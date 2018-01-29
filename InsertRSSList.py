# -*- coding: utf-8 -*-
"""
Created on Thu Jul  6 15:18:21 2017

@author: vgupta
"""
from cassandra.cqlengine import connection
connection.setup(hosts = ['10.116.44.121'], default_keyspace = "test", protocol_version=3)
from fncassandra.RSSFeeds import RSSFeeds
from fncassandra.NewsArticle2 import NewsArticle2
import feedparser
import datetime
import time
import httplib2
import pymmh3
def insertRSS(l):    
    try:
        h = httplib2.Http()
        resp = h.request(l[0], 'HEAD')
        int(resp[0]['status']) < 400
        d = feedparser.parse(l[0])
        link = l[0]
        sourceName = l[1]
        sourceName = sourceName.strip('\n\t')
        sourceName = sourceName.rstrip()
        RSSKeys=[]   
        articleKeys=[]
        metadata =(d.feed)
        metadata2 =d.entries[0] 
        for key in metadata:
            RSSKeys.append(key)
        for key in metadata2:
            articleKeys.append(key)   
        if('title' in RSSKeys):
            title = d.feed.title
        else:
            title = ''
        if('language' in RSSKeys):
            language = d.feed.language
        else:
            language =''
        
        if('published_parsed' in RSSKeys):
            published = d.feed.published_parsed
        else:
            if('updated_parsed' in RSSKeys):
                published = d.feed.updated_parsed
            elif('published_parsed' in articleKeys):
                published = d.entries[0]['updated_parsed']
            else:
                published = time.localtime()
                
        if('update_parsed' in RSSKeys):
            updated = d.feed.updated_parsed
        elif('update_parsed' in articleKeys):
            updated = d.entries[0]['updated_parsed']    
        else:
            updated=published
        updated = str(datetime.datetime(*updated[:6]))
        published = str(datetime.datetime(*published[:6]))
        RSSFeeds.create(link=link,sourcename=sourceName,title=title,language=language,published=published,updated=updated,metadata=str(metadata))

    except Exception as ex:
        #comes here when link doesnot exists
        #print("Exception OF insertRSS",l)
        pass

def insertArticle(l):
    d = feedparser.parse(l[0])
    numberOfEntries = len(d.entries)
    sourceName = l[1]
    sourceName = sourceName.strip('\n\t')
    sourceName = sourceName.rstrip()
    feedLink = l[0]
    num =0

    for eachEntry in range(numberOfEntries):
        try:
            articleKeys=[]
            link = d.entries[eachEntry]['link']
            metadata2 = (d.entries[eachEntry])
            for key in metadata2:
                articleKeys.append(key)
            if('author' in articleKeys):
                author = d.entries[eachEntry]['author']
            else:
                author = ''
            if('title' in articleKeys):
                title = d.entries[eachEntry]['title']
            else:
                title = ''
            
            if('summary' in articleKeys):
                summary = d.entries[eachEntry]['summary']
            else:
                summary = ''
            
            if('content' in articleKeys):
                content = d.entries[eachEntry]['content']           
            else:
                content = ''
            
            if('published_parsed' in articleKeys):
                published = d.entries[eachEntry]['published_parsed']
            elif('updated_parsed' in articleKeys):
                published = d.entries[eachEntry]['updated_parsed']
            else:
                published = time.localtime()
            if('updated_parsed' in articleKeys):
                updated = d.entries[eachEntry]['updated_parsed']
            else:
                updated = published           
            updated = str(datetime.datetime(*updated[:6]))
            published = str(datetime.datetime(*published[:6]))
            
            if(NewsArticle2.objects.filter(sourcename=sourceName,hashprefix =(pymmh3.hash(link)%3),articlelink = link)):
                pass
            else:
                num = num+1
                NewsArticle2.create(sourcename=sourceName,hashprefix =(pymmh3.hash(link)%3) ,articlelink = link, language="EN",state = "new",feedlink=feedLink,author=author,title=title,summary=summary,content=str(content),published=published,updated=str(updated),metadata=str(metadata2),year = 2017)
            
        except Exception as ex:
            #print("exception of insertArticle",sourceName,">>>>>",ex)
            pass
    print("inserted",sourceName,num)
            
        
print("========================================================================================")
print(time.asctime( time.localtime(time.time()) ))
RssFile ="List/RSSLinks_Sources.csv"
RssList =[]
with open(RssFile) as f:
    for line in f:
        LinkRss=[line.split(",")]
        RssList.append(LinkRss[0])
for l in RssList:
    try:
        record = RSSFeeds.get(link=l[0])

        #check for updated date and call insertArticles function and update the updated date
        d = feedparser.parse(l[0])
        try:
            if(d.feed.updated_parsed):
                newUpdatedDate = d.feed.updated_parsed
                newUpdatedDate = datetime.datetime(*newUpdatedDate[:6])
            elif(d.entries[0]['updated_parsed']):
                newUpdatedDate = d.entries[0]['updated_parsed']
                newUpdatedDate = datetime.datetime(*newUpdatedDate[:6])
                    
        except:
            newUpdatedDate= time.localtime()
            newUpdatedDate = datetime.datetime(*newUpdatedDate[:6])
            
        oldUpdatedDate = datetime.datetime.strptime(record.updated, "%Y-%m-%d %H:%M:%S")
        differnceTime = (newUpdatedDate-oldUpdatedDate).total_seconds()
        print("differnceTime",differnceTime)
        if(True):
            #call insert articles
            insertArticle(l)
            #update the time in rss table
            updated = newUpdatedDate
            RSSFeeds.objects(link=l[0]).update(updated=str(newUpdatedDate))
        else:
            pass

    except Exception:
        insertRSS(l)
        insertArticle(l)   
print("========================================================================================")