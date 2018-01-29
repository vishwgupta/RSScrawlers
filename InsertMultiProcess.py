from multiprocessing import Pool
from QueryForEachArticle import QueryForEachArticle
from cassandra.cqlengine import connection
ip =''#insert host connection of db
connection.setup(hosts = [ip], default_keyspace = "test", protocol_version=3)
from fncassandra.NewsArticle import NewsArticle
from fncassandra.NewsArticleTM import NewsArticleTM
import pymmh3
from datetime import datetime
with open("data/embed.vocab") as f:
    vocab_list = map(str.strip, f.readlines())
    vocab_dict = {w: k for k, w in enumerate(vocab_list)}
EmdWords = len(vocab_dict)
print("EMDWORDS",EmdWords)

def InsertStructuredInfo(listofObjects):
    from cassandra.cqlengine import connection
    connection.setup(hosts = ['10.116.44.121'], default_keyspace = "test", protocol_version=3)
    print(listofObjects)
    for num in range(3):
        q = NewsArticle.objects.filter(sourcename=listofObjects,hashprefix=num).limit(None)
        print(len(q))
        Articles = q.filter(state="structinfo")
        for a in range(len(Articles)):
            article = Articles[a]
            try:
                QueryForArticle = QueryForEachArticle(article.articlelink,EmdWords)
                datestring = article.updated
                dt = datetime.strptime(datestring, '%Y-%m-%d %H:%M:%S')
                date = str(dt.month)+"-"+str(dt.year)
                BOWnGrams,BOWnGramsTitle,NER,d2v,CentroidOfDocument,key_words = QueryForArticle.StructuredDataForRSSLink(article.content,article.title)
                CentroidOfDocument = list(CentroidOfDocument)#.tolist()
                d2v = list(d2v)
                #print("just before insertion")
                NewsArticleTM.create(sourcename=listofObjects,hashprefix=num,date=date,articlelink=article.articlelink,language="EN",key_words = key_words,named_entities=NER,bag_of_words=BOWnGrams,bag_of_words_title=BOWnGramsTitle,document_vector =d2v,centroid_of_document=CentroidOfDocument)
                #NewsArticle.objects(sourcename=article.sourcename,hashprefix =(pymmh3.hash(article.articlelink)%3),articlelink = article.articlelink,language="EN").update(state="structinfo")
            except Exception as ex:
                print("in exception",ex)
                pass
    return "none"

if __name__ == '__main__':    
    try:
        #listofObjects = [,\n']
        #
        listofObjects = ['CNN','CBS NEWS','The New York Times','Reuters','Vox','abc News','The Economist','TIME','THE Fiscal TIMES','The Atlantic','CNBC'] 
        listofObjects = ['THE HUFFINGTON POST','THE HILL','NBC News','The Washington Post','The CHRISTIAN SCIENCE MONITOR','THE NATION','the guardian','USA TODAY','The Wall Street Journal','POLITICO','Al Jazeera']             
        listofObjects = ['Slate','AP','PBS','npr','10 TV','10 news','The Telegraph','Business Insider','11 ALIVE','The Intercept']
        print(listofObjects)
        #for a in listofObjects:
        #    print(a.link)
        #listofObjects = [Articles.objects.limit(20)]
        pool = Pool(processes=11)        
        print(pool.map(InsertStructuredInfo, listofObjects))
    except Exception as ex:
        print("here in main",ex)