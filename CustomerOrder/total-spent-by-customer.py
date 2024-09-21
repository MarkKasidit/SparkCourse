from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalSpent")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    customerSpent = float(fields[2])
    return (customerID, customerSpent)

lines = sc.textFile("file:///sparkcourse/CustomerOrder/customer-orders.csv")
parsedLines = lines.map(parseLine)
sumSpent = parsedLines.reduceByKey(lambda x, y: x + y)
fliped = sumSpent.map(lambda (x, y): (y, x)).sortByKey()
results = fliped.collect()

for result in results:
    print(str(result[0]) + f"\t{result[1]}")
