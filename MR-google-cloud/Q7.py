#!/bin/python
"""
Adapted from https://github.com/Yelp/mrjob
"""

from mrjob.job import MRJob
import re
import heapq
from lxml import etree
from io import StringIO, BytesIO
import mwparserfromhell
SEG_RE = re.compile(r"(.+?)\|")
regex1=re.compile(r"category:.*")
regex2=re.compile(r"template:.*")
regex3=re.compile(r"talk:.*")
regex4=re.compile(r"help:.*")
regex5=re.compile(r"file:.*")

class MRParse(MRJob):

    def mapper_init( self ):
        #self.parser = etree.XMLParser()
        self.status=0
        self.page=''

    def mapper( self , _ , line ):
        
        if '<page>' in line:
            self.status=1
        
        if self.status:
            self.page+=line
            if '</page>' in line:
                
                self.status=0
                try:
                    root = etree.fromstring(self.page) # the parser might fail
                    
                    if root.find('revision/text') is not None and root.find('title') is not None:
                        #title=root.find('title').text.decode('utf-8').lower()
                        title=root.find('title').text.lower()
                        if any(regex.match(title) for regex in [regex1, regex2, regex3, regex4, regex5])==False: # and title=='april':
                            
                            text=root.find('revision/text').text
                            wikicode=mwparserfromhell.parse(text)
                            wikilinks= wikicode.filter_wikilinks() # wikilink object
                            #links=list(set([str(link.title).decode('utf-8').lower() for link in wikilinks]))
                            links=list(set([str(link.title).lower() for link in wikilinks]))
                            links=filter(lambda x: any(regex.match(x) for regex in [regex1, regex2, regex3, regex4, regex5])== False,links)
                            if re.search(SEG_RE, title):
                                title=re.search(SEG_RE, title).group(1)    
                            weight=1.0/(len(links)+10)

                            for link in links:                                                
                                yield title, (link, weight, 'link')
                                yield link, (title, weight, 'reverse')
                    self.page=''# Reset the string
                                
                except: 
                    self.page=''
                    
                
    def reducer(self, key, values):  #Multiplication

        start=((link, weight) for link, weight, k in values if k=='link')
        end=((link, weight) for link, weight, k in values if k=='reverse')       
        for title1, weight1 in start:
            for title2, weight2 in end:
                if title1!=title2:
                    result=tuple(sorted([title1,title2]))                    
                    yield result, weight1*weight2       
                    
class Summation(MRJob):

    def reducer(self, key, counts):  
        yield (key, sum(counts))

class Top(MRJob): #select top 100 double links
  
    def mapper_init(self):
        self.h=[]
    
    def mapper(self, dlink, count):
    
        if len(self.h)<100:
            heapq.heappush(self.h,(count,dlink))
        elif self.h[0][0]<count and len(self.h)==100:
            heapq.heappop(self.h)
            heapq.heappush(self.h,(count,dlink))

    def mapper_final(self):
        for count, dlink in self.h:
            yield (1, (count, dlink))
  

    def reducer_init(self):
        self.top=[]
        
    def reducer(self, _, word_count):
        for count, dlink in word_count:
            if len(self.top)<100:
                heapq.heappush(self.top, (count, dlink))
            elif self.top[0][0]<count and len(self.top)==100:
                heapq.heapreplace(self.top,(count, dlink))
  
    def reducer_final(self):
        
        for count, dlink in self.top:
            yield dlink, count
    

class SteppedJob(MRJob):

    def steps(self):
        return MRParse().steps()  + Summation().steps() + Top().steps()

if __name__ == '__main__':
    SteppedJob.run()