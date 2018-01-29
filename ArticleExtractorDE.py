#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 28 10:54:56 2017

@author: vgupta
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Jul 10 14:23:45 2017

@author: vgupta
"""

from newspaper import Article
from bs4 import BeautifulSoup
import requests

class ArticleExtractorDE:
    def newspaperAPI(self):
        try:
            a = Article(self.url, language='de')
            a.download()
            a.parse()
            content = a.text
            title = a.title
            date = a.publish_date
        except:
            pass
        return content,title,date
        
    
    def scrapyResult(self):
        try:
            artBS4 = []
            url = self.url
            page = requests.get(url)
            soup = BeautifulSoup(page.content, 'html.parser')

            para = soup.find_all('p',{ "class" : None })
            for a in para:
                text = a.get_text()
                artBS4.append(text)
            artBS4 = "".join(artBS4)        
        except:
            pass
        return artBS4,None,None    
        
    def bestContent(self):
        try:
            #print("in best content")
            contentNewsPaper,title,date = self.newspaperAPI()
            lenContentNewsPaper = len(contentNewsPaper)
        
            contentScrapy = self.scrapyResult()
            lenContentScrapy = len(contentScrapy)
            if(lenContentNewsPaper > lenContentScrapy):
                print("returned newspaper")
                return contentNewsPaper,title,date
            else:
                print("returned scrapy")
                return contentScrapy,title,date
        except:
            pass
    def __init__(self, url):
        self.url = url