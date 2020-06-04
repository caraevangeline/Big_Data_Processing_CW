from mrjob.job import MRJob
import re
import time
import statistics


#This line declares the class Lab1, that extends the MRJob format.
class gas(MRJob):

# this class will define two additional methods: the mapper method goes here
  def mapper(self, _, line):
        fields = line.split(",")
        try:
            if (len(fields)==7):
               time_epoch = int(fields[6])
               gas_price = int(fields[5])
               gas = int(fields[4])
               #price = int(gas_price/gas)
               #count = int(fields[8])
               days = time.strftime("%Y-%m",time.gmtime(time_epoch)) 
               #years = time.strftime("%y",time.gmtime(time_epoch))
               yield (days, gas_price)
        except:
            pass
            
  def reducer(self, word, counts):
       yield (word, statistics.mean(counts))
 
 
if __name__ == '__main__':
    gas.run()
