import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

if __name__ == "__main__":
    """
        Usage:  Create (empty) table to hold all transactions

                To execute:
./bin/spark-submit \
../Git/moneytrackerapp/CreateTransactionsTable.py

    """

    spark = SparkSession\
        .builder\
        .appName("CreateTransactionsTable")\
        .getOrCreate()

    spark.sql("""
        CREATE TABLE if not exists
        cc_transactions (
            transaction_date    Date, 
            post_date           Date,
            description         String, 
            category            String,
            Type                String,
            Amount              Double,
            Memo                String
        ) USING CSV""")
 
    spark.sql("""
        insert into table cc_transactions values (null, null, 'yes', 'no', 'maybe', 5.19, 'yes')
        """)
    ccStCount = cc_transactions.count()

    print("cc_transactions count: %f" % ccStCount)


# Chase:        Transaction Date,   Post Date,  Description,    Category,   Type,   Amount, Memo
# Barclays:     Transaction Date,               Description,    Category,           Amount





    spark.stop()
