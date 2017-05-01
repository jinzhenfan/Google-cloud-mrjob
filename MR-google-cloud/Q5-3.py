#improve efficiency of calculating mean, count, and std.
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
WORD_RE = re.compile(r"[\w]+")
import math,random
import numpy as np
reservior=1000
class LinkCount(MRJob):

    def mapper_init( self ):
        #self.parser = etree.XMLParser()
        self.status=0
        self.page=''
        self.pagecount=0
        self.linkcount=0
        self.square=0
        self.h=[]

    def mapper( self , _ , line ):
        if '<page>' in line:
            self.status=1
          
        if self.status:
            self.page+=line
            if '</page>' in line:
                self.status=0
                try:
                    root = etree.fromstring(self.page) # the parser might fail 
                    
                    if root.find('revision/text') is not None and root.find('id') is not None:
                        text=root.find('revision/text').text
                        pid= root.find('id').text
                        wikicode = mwparserfromhell.parse(text)                 
                        links = list(set(" ".join(fragment.split()) for fragment in wikicode.filter_wikilinks()))
                        self.pagecount+=1
                        self.linkcount+=len(links)
                        self.square+=len(links)**2
                        r = random.random()
                        if len(self.h)<reservior:
                            heapq.heappush(self.h, (r, len(links)))
                        elif self.h[0][0]<r and len(self.h)==reservior:
                            heapq.heapreplace(self.h,(r, len(links)))
      
                    self.page=''# Reset the string
                    pid=''
                                
                except: 
                    self.page=''
                    pid=''
                    
    def mapper_final(self):
        
        yield 1, (self.pagecount, self.linkcount, self.square, self.h)
              
    def reducer_init(self):
        self.top=[]
        self.totalpage=0
        self.totallink=0
        self.totalsquare=0

        
    def reducer(self, alias, mappercount):  
        for pagecount, linkcount, square, heaps in mappercount:
            self.totalpage+=pagecount
            self.totallink+=linkcount
            self.totalsquare+=square
            for r, links in heaps:
                r2 = random.random()
                if len(self.top)<reservior:
                    heapq.heappush(self.top, (r2, links))
                elif self.top[0][0]<r2 and len(self.top)==reservior:
                    heapq.heapreplace(self.top, (r2, links))


    def reducer_final(self):
        quantile=[]
        avg_links=self.totallink*1.0/self.totalpage
        avg_square=self.totalsquare*1.0/self.totalpage
        std=math.sqrt(avg_square-avg_links**2)

        for r2, links in self.top:
            quantile.append(links)
            
        values=np.array(quantile)
        
        q1 = np.percentile(values, 25) # return 50th percentile, e.g median.

        q2 = np.percentile(values, 50) # return 50th percentile, e.g median.
        
        q3 = np.percentile(values, 75) # return 50th percentile, e.g median.

        yield "Summary", {'Total number of articles': self.totalpage, "Avg links per pages": avg_links, "standard deviation": std, '25%': q1, "50%": q2, "75%": q3}      
    

class SteppedJob(MRJob):

  def steps(self):
    return LinkCount().steps()

if __name__ == '__main__':
  SteppedJob.run()
