chase_prime_schema = StructType([
  StructField("TRANSACTION_DATE", StringType(), False),
  StructField("POST_DATE", StringType(), False),
  StructField("DESCRIPTION", StringType(), False),
  StructField("CATEGORY", StringType(), True),
  StructField("TYPE", StringType(), False),
  StructField("AMOUNT", StringType(), False),
  StructField("MEMO", StringType(), True)
])

barclays_schema = StructType([
  StructField("TRANSACTION_DATE", StringType(), False),
  StructField("DESCRIPTION", StringType(), False),
  StructField("CATEGORY", StringType(), False),
  StructField("AMOUNT", StringType(), False)
])