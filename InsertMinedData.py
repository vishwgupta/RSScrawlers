#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec  3 17:21:26 2017

@author: vgupta
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec  1 11:40:07 2017

@author: vgupta
"""
import re
from nltk.tokenize import sent_tokenize
from nltk.corpus import stopwords
from nltk import word_tokenize
import numpy as np
from nltk.tag.stanford import StanfordNERTagger
from collections import Counter, OrderedDict
import gensim
from nltk import RegexpTokenizer
from gensim.models.phrases import Phraser
from cassandra.cqlengine import connection
ip =''#insert host connection of db
connection.setup(hosts = [ip], default_keyspace = "test", protocol_version=3)
from fncassandra.NewsArticle import NewsArticle
from fncassandra.NewsArticleTM import NewsArticleTM
from datetime import datetime
import pymmh3
from fncassandra.Source import Source
with open("data/embed.vocab") as f:
    vocab_list = map(str.strip, f.readlines())
    vocab_dict = {w: k for k, w in enumerate(vocab_list)}
EmdWords = len(vocab_dict)
bigram = Phraser.load('mymodel/bigram_phraser_wikien2017')
trigram = Phraser.load('mymodel/trigram_phraser_wikien2017')
dim = 200
W = np.memmap("data/embed.dat", dtype=np.double, mode="r", shape= ((EmdWords, dim)))
st = StanfordNERTagger('/home/IAIS/vgupta/stanford-ner/classifiers/english.all.3class.distsim.crf.ser.gz','/home/IAIS/vgupta/stanford-ner/stanford-ner.jar',encoding='utf-8')

d2v_model = gensim.models.doc2vec.Doc2Vec.load('doc2vec.model')

def nlp_clean(data):
   new_data = []
   tokenizer = RegexpTokenizer(r'\w+')
   stopword_set = set(stopwords.words('english'))
   for d in data:
      new_str = d.lower()
      dlist = tokenizer.tokenize(new_str)
      dlist = list(set(dlist).difference(stopword_set))
      new_data.append(dlist)
   return new_data
def doc2vec(link,docname):
        data = [docname]            
        data = nlp_clean(data)
        docvec = d2v_model.infer_vector(doc_words=data[0], steps=20, alpha=0.025)
        return docvec

def CentroidOfDocument(BOWnGrams,EmdWords):
        dim = 200
        BOW = [x for x in BOWnGrams]
        with open("data/embed.vocab") as f:
            vocab_list = map(str.strip, f.readlines())
        vocab_dict = {w: k for k, w in enumerate(vocab_list)}
        numberWords = len(BOW)
        VecMatrixWords =np.zeros((numberWords, dim))
        for j,t in enumerate(BOW):
            VecMatrixWords[j] = (W[[vocab_dict[t]]])
        centroid =np.zeros((1, dim))
        for j in range(dim):
            for i in range(numberWords):
                centroid[0][j] =  centroid[0][j]+(VecMatrixWords[i][j])
            centroid[0][j]=  centroid[0][j]/dim

        return centroid[0]

def constructNGramsBOW(docName):       
        text = docName
        stopsWords = set(stopwords.words('english'))
        text = re.sub("[^a-zA-Z]", " ", text.lower()) 
        sent_tokenize_list = sent_tokenize(text)
        with open("data/embed.vocab") as f:
            vocab_list = map(str.strip, f.readlines())
        for line in sent_tokenize_list:
            sent = word_tokenize(line)
            line = trigram[bigram[sent]]
            line = [w for w in line if not w in stopsWords ]
        with open("data/embed.vocab") as f:
            vocab_list = map(str.strip, f.readlines())
            vocab_dict = {w: k for k, w in enumerate(vocab_list)}
        
        BOWNgram = [token for token in line if token in vocab_dict.keys()]
        BOWNgram = Counter(BOWNgram)

        return OrderedDict(BOWNgram)
def keywords(docName):
        """Get the top 10 keywords and their frequency scores ignores blacklisted
        words in stopwords, counts the number of occurrences of each word, and
        sorts them in reverse natural order (so descending) by number of
        occurrences.
        """
        
        NUM_KEYWORDS = 10
        text = docName
        # of words before removing blacklist words
        if text:
            num_words = len(text)
            text = re.sub("[^a-zA-Z]", " ", text)
            stopsWords = set(stopwords.words('english'))

            text = [x for x in text.lower().split() if x not in stopsWords]
            freq = {}
            for word in text:
                if word in freq:
                    freq[word] += 1
                else:
                    freq[word] = 1

            min_size = min(NUM_KEYWORDS, len(freq))
            keywords = sorted(freq.items(),key=lambda x: (x[1], x[0]),reverse=True)
            keywords = keywords[:min_size]
            keywords = dict((x, y) for x, y in keywords)

            for k in keywords:
                articleScore = keywords[k] * 1.0 / max(num_words, 1)
                keywords[k] = articleScore * 1.5 + 1

            return OrderedDict(keywords)
        else:
            return dict()
def splitSentence(docName):
        sent_tokenize_list = sent_tokenize(docName)
        return sent_tokenize_list           

def InsertStructuredInfo(listofSources):
    for listofObjects in listofSources:
        for num in range(3):
            q = NewsArticle.objects.filter(sourcename=listofObjects,hashprefix=num).limit(None)
            
            Articles = q.filter(state="content").limit(None)
            print(listofObjects,num,len(Articles))
            for a in range(len(Articles)):
                article = Articles[a]
                try:
                    #QueryForArticle = QueryForEachArticle(article.articlelink,EmdWords)
                    datestring = article.updated
                    dt = datetime.strptime(datestring, '%Y-%m-%d %H:%M:%S')
                    date = str(dt.month)+"-"+str(dt.year)
                    BOWnGrams = constructNGramsBOW(article.content)
                    BOWnGramsTitle = constructNGramsBOW(article.title)
                    #ner = NER(article.content)
                    ner = ""
                    d2v = doc2vec(article.articlelink,article.content)
                    centroidOfDocument = CentroidOfDocument(BOWnGrams,EmdWords)
                    key_words = keywords(article.content)
                    #NewsArticleTM.create(sourcename=listofObjects,hashprefix=num,date=date,articlelink=article.articlelink,language="EN",key_words = key_words,bag_of_words=BOWnGrams,bag_of_words_title=BOWnGramsTitle,document_vector =d2v,centroid_of_document=centroidOfDocument)
                    #CentroidOfDocument = list(CentroidOfDocument)#.tolist()
                    #d2v = list(d2v)
                    NewsArticleTM.create(sourcename=listofObjects,hashprefix=(pymmh3.hash(article.articlelink)%3),date=date,articlelink=article.articlelink,language="EN",key_words = key_words,named_entities=ner,bag_of_words=BOWnGrams,bag_of_words_title=BOWnGramsTitle,document_vector =d2v,centroid_of_document=centroidOfDocument)
                    NewsArticle.objects(sourcename=article.sourcename,hashprefix =article.hashprefix,articlelink = article.articlelink,language="EN").update(state="structinfo")
                except Exception as ex:
                    print("in exception",ex)
                    pass

sourceList=[]
for source in Source.objects.limit(None):
    sourceList.append((source.name))
InsertStructuredInfo(sourceList)
