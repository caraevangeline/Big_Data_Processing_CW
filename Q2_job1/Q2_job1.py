
from mrjob.job import MRJob
import re

class Q2_Job1(MRJob):



    def mapper(self, _, line):
        fields = line.split(",")
        try:
            if(len(fields)==7):
                address = fields[2]
                value = int(fields[3])
                yield(address,value)

        except:
            pass

    def combiner(self,word,count):
        yield(word,sum(count))

    def reducer(self,word,counts):
        k=sum(counts)
        yield(word,k)


if __name__ == '__main__':
    Q2_Job1.run()


