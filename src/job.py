from pyspark.sql import SparkSession
from extract import extract
from metadata.constant import CHASE_PRIME_SCHEMA, BARCLAYS_SCHEMA
from transform import monthly_totals 
from load import load

"""
./bin/spark-submit ../Git/moneytrackerapp/src/job.py
"""

SPARK = SparkSession.builder.appName("moneytracker").getOrCreate()

#### Extract ####
chase_file_location = "./data/cc-transactions/Chase7437_Activity20210713_20230713_20230713.CSV"
barclays_file_location = "./data/cc-transactions/CreditCard_20230101_20230711.csv"

chaseprime_df = extract(SPARK, CHASE_PRIME_SCHEMA, chase_file_location)
barclays_df = extract(SPARK, BARCLAYS_SCHEMA, barclays_file_location)

monthly_totals_df = monthly_totals(chaseprime_df, barclays_df)
monthly_totals_df.printSchema()

x = chaseprime_df.count()
y = barclays_df.count()

print("chaseprime_df count: %f" % x)
print("barclays_df count: %f" % y)

# #### Load Data ####
# # MySQL 
# load("JDBC", barclays_df, "barclays")

# # FileSystem
load("CSV", monthly_totals_df, "./data/cc-transactions/output/monthly_totals/")


