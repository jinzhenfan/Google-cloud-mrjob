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
    
  def reducer(self, word, counts):  # each reducer get a bunch of tuples that may contain duplicated keys 
    yield (word, sum(counts))


class SecondStep(MRJob):

  def reducer_init(self):
      self.top=[]
        
  def reducer(self, word, ncount):
      heapq.heappush(self.top, (-sum(ncount), word)) #  Or keeping only a heap with 100 and pop
  
  def reducer_final(self):
      for i in range(100):
          ncount, word= heapq.heappop(self.top)
          yield word, -ncount

class SteppedJob(MRJob):
  """
  A two-step job that first runs FirstStep's MR and then SecondStep's MR
  """
  def steps(self):
    return FirstStep().steps() + SecondStep().steps()

if __name__ == '__main__':
  SteppedJob.run()