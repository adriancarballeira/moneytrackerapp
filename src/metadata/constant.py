from pyspark.sql.types import StructField, StructType, StringType

CHASE_PRIME_SCHEMA = StructType([
  StructField("TRANSACTION_DATE", StringType(), False),
  StructField("POST_DATE", StringType(), False),
  StructField("DESCRIPTION", StringType(), False),
  StructField("CATEGORY", StringType(), True),
  StructField("TYPE", StringType(), False),
  StructField("AMOUNT", StringType(), False),
  StructField("MEMO", StringType(), True)
])

BARCLAYS_SCHEMA = StructType([
  StructField("TRANSACTION_DATE", StringType(), False),
  StructField("DESCRIPTION", StringType(), False),
  StructField("CATEGORY", StringType(), False),
  StructField("AMOUNT", StringType(), False)
])