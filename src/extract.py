from pyspark.sql import SparkSession

def extract(spark: SparkSession, schema: str, source: str):

    output_df = spark\
        .read\
        .schema(schema)\
        .option("header", "true")\
        .csv(source)        

    return output_df