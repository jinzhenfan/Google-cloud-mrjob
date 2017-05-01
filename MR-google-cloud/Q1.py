#!/bin/python
"""
Adapted from https://github.com/Yelp/mrjob
"""

from mrjob.job import MRJob
import re
import heapq

WORD_RE = re.compile(r"[\w]+")

class FirstStep(MRJob):
  def mapper(self, _, line):  
    for word in WORD_RE.findall(line):
      yield (word.lower(), 1)   
    
  def combiner(self, word, counts): 
    yield (word, sum(counts))
    
  def reducer(self, word, counts):  
    yield (word, sum(counts))


class SecondStep(MRJob):
  
  def mapper_init(self):
      self.h=[]
    
  def mapper(self, word, count):
    
    if len(self.h)<100:
        heapq.heappush(self.h,(count,word))
    elif self.h[0][0]<count and len(self.h)==100:
        heapq.heappop(self.h)
        heapq.heappush(self.h,(count,word))
  
  def mapper_final(self):
      for count, word in self.h:
          yield (1, (word, count))
  

  def reducer_init(self):
      self.top=[]
        
  def reducer(self, _, word_count):
      for pair in word_count:
          heapq.heappush(self.top, (-pair[1], pair[0]))
  
  def reducer_final(self):
      for i in range(100):
          ncount, word= heapq.heappop(self.top)
          yield word, -ncount

class SteppedJob(MRJob):

  def steps(self):
    return FirstStep().steps() + SecondStep().steps()

if __name__ == '__main__':
  SteppedJob.run()