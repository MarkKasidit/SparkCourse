from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("TotalSpentCustomer").master("local[*]").getOrCreate()

customerOrderSchema = StructType([
    StructField("CustomerID", IntegerType(), True),
    StructField("ItemID", IntegerType(), True),
    StructField("Spent", FloatType(), True)
])

df = spark.read.schema(customerOrderSchema).csv("file:///sparkcourse/CustomerOrder/customer-orders.csv")
# df.printSchema()

totalByCustomer = df.select("CustomerID", "Spent").groupBy("CustomerID").agg(func.round(func.sum("Spent"), 2).alias("SumSpent")).sort("SumSpent")
totalByCustomer.show(totalByCustomer.count())