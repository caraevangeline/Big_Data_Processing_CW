
from mrjob.job import MRJob
import re
import time
import statistics


#This line declares the class Lab1, that extends the MRJob format.
class Q1(MRJob):

# this class will define two additional methods: the mapper method goes here
  def mapper(self, _, line):
        fields = line.split(",")
        try:
            if (len(fields)==9):
               time_epoch = int(fields[7])
               count = int(fields[8])
               months = time.strftime("%m",time.gmtime(time_epoch)) 
               years = time.strftime("%y",time.gmtime(time_epoch))
               yield ((months,years),count)
        except:
            pass
            
  def reducer(self, word, counts):
       yield (word, sum(counts))
 
 
if __name__ == '__main__':
    Q1.run()
