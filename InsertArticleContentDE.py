#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 28 10:53:18 2017

@author: vgupta
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Jul 10 15:09:04 2017

@author: vgupta
"""

from cassandra.cqlengine import connection
import datetime
import time
import httplib2
import urllib.request
import pymmh3
ip=""# insert connection ip to the db
connection.setup(hosts = ip, default_keyspace = "test", protocol_version=3)

from fncassandra.NewsArticle2 import NewsArticle2
from fncrawler.ArticleExtractorDE import ArticleExtractorDE

def checkOnContent():
    q = NewsArticle2.objects.filter(state='new').limit(None)
    print(len(q))
    q = q.filter(language="DE").allow_filtering()
    print(len(q))
    for a in range(len(q)):
        article = q[a]
        try:
            art = ArticleExtractorDE(article.articlelink)
            content,title,date= art.bestContent()
            titleFromRSS = article.title
            if(len(titleFromRSS)<len(title)):
                NewsArticle2.objects(sourcename=article.sourcename,hashprefix =(pymmh3.hash(article.articlelink)%3),articlelink = article.articlelink,language="DE").update(title=title)
            else:
                pass
            #update contentFetched to Yes
            NewsArticle2.objects(sourcename=article.sourcename,hashprefix =(pymmh3.hash(article.articlelink)%3),articlelink = article.articlelink,language="DE").update(content=content)
            NewsArticle2.objects(sourcename=article.sourcename,hashprefix =(pymmh3.hash(article.articlelink)%3),articlelink = article.articlelink,language="DE").update(state='content')
        except Exception as ex:
            print("exception",article.articlelink,ex)
            try:
                timeNow = time.localtime()
                timeNow = datetime.datetime(*timeNow[:6])
                updatedTimeForArticle = datetime.datetime.strptime(article.updated, "%Y-%m-%d %H:%M:%S")
                h = httplib2.Http()
                resp = h.request(article.articlelink, 'HEAD')
                differenceTime = (timeNow-updatedTimeForArticle).total_seconds()
                if(differenceTime>2592000 and (article.contentFetched) =='No'and int(resp[0]['status']) > 400):
                    print("article deleted", article.articlelink)
                    #NewsArticle.objects(sourcename=article.sourcename,hashprefix =(pymmh3.hash(article.articlelink)%3),articlelink = article.articlelink,language="EN").delete()
                else:
                    print(article.articlelink)
                    pass
            except:
                #NewsArticle.objects(sourcename=article.sourcename,hashprefix =(pymmh3.hash(article.articlelink)%3),articlelink = article.articlelink,language="EN").delete()
                pass
            
checkOnContent()

