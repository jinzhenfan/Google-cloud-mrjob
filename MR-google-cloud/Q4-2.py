from mrjob.job import MRJob
import re
import heapq
from lxml import etree
from io import StringIO, BytesIO
import mwparserfromhell
import math
n_gram=1
class MRParser(MRJob):

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
                    if root.find('revision/text') is not None:
                        text=root.find('revision/text').text
                        wikicode = mwparserfromhell.parse(text)
                        text = " ".join(" ".join(fragment.value.split()) for fragment in wikicode.filter_text())
                        for i in range(len(text)-n_gram):
                            yield (text[i:i+n_gram],1)
                        #for word in text:                            
                        #    yield (word, 1)
                    self.page=''# Reset the string
                                
                except: 
                    self.page=''


    def combiner(self, word, counts): 
        yield (word, sum(counts))
    #multiplication
    def reducer(self, word, counts):  
        yield (word, sum(counts))            
    
#Counting
class Entropy(MRJob):
    def mapper(self, word, count):
        c=math.log(count,2)
        countN=count
        yield (1, (count*c,countN)) # Do calculation for N and nlogn in the same mapreduce
        
    def reducer(self, word, values):
        nlogn=0
        N=0
        for v in values:
            nlogn+=v[0]
            N+=v[1]
        result=math.log(N,2)-1.0/N*nlogn
        result=result/n_gram
        yield (n_gram, result)
        

class SteppedJob(MRJob):

  def steps(self):
    return MRParser().steps() + Entropy().steps() #+ SecondStep().steps()

if __name__ == '__main__':
  SteppedJob.run()