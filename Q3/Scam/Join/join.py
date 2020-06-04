from mrjob.job import MRJob

class join1(MRJob):

    def mapper(self, _, line):
       try:
            #one mapper, we need to first differentiate among both types
            if(len(line.split('\t'))== 2):
                fields = line.split('\t')
                join_key = fields[1]
                join_key = join_key[3:-1]
                join_value = fields[0]
                yield (join_key, (join_value,1))


            if(len(line.split(','))== 7):
                fields = line.split(',')
                join_key = fields[2]
                join_value = int(fields[3])
                yield (join_key,(join_value,2))
       except:
            pass
            

    def reducer(self, key, values):

        category = None
        amount = None
        for value in values:
            if value[1] == 1:
                category = value[0]
            if value[1] == 2:
                amount = value[0]
        if amount is not None and category is not None:
           yield ((key,category), amount)

if __name__ == '__main__':
    join1.run()
