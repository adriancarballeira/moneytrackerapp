import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

if __name__ == "__main__":
    """
        Usage:  Reads in .csv of transactions downloaded from Chase Prime (credit card purchases).
                Analyzes how many dollars are spent where.
                Returns summary to user.

                To execute:
./bin/spark-submit \
../Git/moneytrackerapp/Chase-Prime.py

    """

    spark = SparkSession\
        .builder\
        .appName("Chase-Prime")\
        .getOrCreate()

    ccStatement = spark\
        .read\
        .option("inferSchema", "true")\
        .option("header", "true")\
        .csv("./data/cc-transactions/Chase7437_Activity20210713_20230713_20230713.CSV")

    #ccStatement.createOrReplaceTempView("ccStatement")

    ccStatement = ccStatement\
        .withColumn("transaction_date", f.to_date("Transaction Date", "MM/dd/yyyy"))\
        .withColumn("post_date", f.to_date("Post Date", "MM/dd/yyyy"))


# Chase:        Transaction Date,   Post Date,  Description,    Category,   Type,   Amount, Memo
# Barclays:     Transaction Date,               Description,    Category,           Amount

    
    # ccStatement.selectExpr("`Transaction Date` AS transaction_date",
    #     "`Post Date` AS transaction_date",
    #     "Description AS Description",
    #     "Category AS category",
    #     "Type AS type",
    #     "Amount AS Amount",
    #     "Memo AS Memo").write.insertInto("cc_transactions", overwrite=False)



    ccStCount = ccStatement.count()

    # ccTotals = ccStatement\
    #     .withColumn("transaction_date", f.to_date("Transaction Date", "MM/dd/yyyy"))\
    #     .withColumn("transaction_month", f.trunc("transaction_date", "month"))\
    #     .groupBy("transaction_month")\
    #     .sum("amount")\
    #     .withColumnRenamed("sum(amount)", "amount_total")\
    #     .orderBy("amount_total")

    # ccTotals = ccTotals\
    #     .withColumn("amount_total", f.round(ccTotals.amount_total, 2))

    # ccTotals.show()

    print("CC statement transaction count: %f" % ccStCount)

    spark.stop()
