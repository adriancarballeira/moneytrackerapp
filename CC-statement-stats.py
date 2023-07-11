import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, abs 


if __name__ == "__main__":
    """
        Usage: helloworld
    """
    spark = SparkSession\
        .builder\
        .appName("helloworld")\
        .getOrCreate()

    ccStatement = spark\
        .read\
        .option("inferSchema", "true")\
        .option("header", "true")\
        .csv("./data/Barclays/CreditCard_20230417_20230710.csv")

    ccStatement.createOrReplaceTempView("ccStatement")

    ccStCount = ccStatement.count()
    #ccStatement.printSchema()

    ccTotals = ccStatement\
        .withColumn("amount", abs(ccStatement.Amount))\
        .groupBy("Description")\
        .sum("amount")\
        .withColumnRenamed("sum(amount)", "amount_total")\
        .sort(desc("amount_total"))

    ccTotals.show()

    print("CC statement transaction count: %f" % ccStCount)
    #print(ccTotals)

    spark.stop()