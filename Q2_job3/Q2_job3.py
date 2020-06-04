
from mrjob.job import MRJob
import re
import time
import statistics
class Q2_job3(MRJob):

# this class will define two additional methods: the mapper method goes here
  def mapper(self, _, line):
      try:
         fields = line.split()
         address = fields[0]
         address = address[2:-2]
         value = int(fields[2])
         yield (None, (address, value))
      except:
         pass
            

  def combiner(self, _, values):
       sorted_values = sorted(values, reverse = True, key = lambda tup:tup[1])
       i=0
       for value in sorted_values:
           yield ("top", value)
           i += 1
           if i >= 10:
              break

  def reducer(self, _, values):
       sorted_values = sorted(values, reverse = True, key = lambda tup:tup[1])
       i=0
       for value in sorted_values:
           yield ("{} - {} ".format(value[0],value[1]),None)
           i += 1
           if i >= 10:
              break


      #this part of the python script tells to actually run the defined MapReduce job. Note that Lab1 is the name of the class
if __name__ == '__main__':
    Q2_job3.run()
