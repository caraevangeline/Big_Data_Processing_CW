import pyspark

def transaction(line):
     try:
         fields = line.split(',')
         if len(fields) != 7:
              return False
         int(fields[3])
         return True
     except:
         return False

def contract(line):
     try:
         fields = line.split(',')
         if len(fields) != 5:
              return False
         return True
     except:
         return False

sc = pyspark.SparkContext()

transactions = sc.textFile("/data/ethereum/transactions")
valid_transactions = transactions.filter(transaction)
map_transactions = valid_transactions.map(lambda l: (l.split(',')[2], int(l.split(',')[3])))
agg_transactions = map_transactions.reduceByKey(lambda a,b: a+b)
contracts = sc.textFile("/data/ethereum/contracts")
valid_contracts = contracts.filter(contract)
map_contracts = valid_contracts.map(lambda l: (l.split(',')[0], None))
joined = agg_transactions.join(map_contracts)

t10 = joined.takeOrdered(10, key = lambda k: -k[1][0])

for value in t10:
    print("{} - {}".format(value[0], value[1][0]))
