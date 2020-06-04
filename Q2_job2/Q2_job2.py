from mrjob.job import MRJob

class Q2_job2(MRJob):

    def mapper(self, _, line):
       try:
            #one mapper, we need to first differentiate among both types
            if(len(line.split('\t'))==2):
                fields = line.split('\t')
                join_key = fields[0]
                join_key = join_key[1:-1]
                join_value = int(fields[1])
                yield (join_key, (join_value,1))


            if(len(line.split(','))==5):
                fields = line.split(',')
                join_key = fields[0]
                join_value = fields[3]
                yield (join_key,(join_value,2))
       except:
            pass
            

    def reducer(self, key, values):

        block = None
        amount = None
        for value in values:
            if value[1] == 1:
                amount = value[0]
            if value[1] == 2:
                block = value[0]
        if amount is not None and block is not None:
           yield ((key,block), amount)

if __name__ == '__main__':
    Q2_job2.run()


