from mrjob.job import MRJob
import re
import time
import statistics
class top(MRJob):

# this class will define two additional methods: the mapper method goes here
  def mapper(self, _, line):
      try:
         fields = line.split()
         address = fields[1]
         address = address[3:-4]
         value = int(fields[2])
         yield (address, value)
      except:
         pass
            

  def reducer(self, word, counts):
       yield (word, sum(counts))


      #this part of the python script tells to actually run the defined MapReduce job. Note that Lab1 is the name of the class
if __name__ == '__main__':
    top.run()
