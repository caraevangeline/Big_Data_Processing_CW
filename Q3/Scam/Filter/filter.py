from mrjob.job import MRJob

class filter1(MRJob):

  def mapper(self, _,line):
         fields = line.split(",")
         try:
           # if(len(fields)== ):
             scam = fields[6]
             scam = scam[14:-1]
             address = fields[0]
             yield(scam, address)

         except:
             pass

  #def reducer(self, word, counts):
        #yield(word, sum(counts))

if __name__ == '__main__':
    filter1.run()
