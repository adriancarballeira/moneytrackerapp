from pyspark.sql import DataFrame
import pyspark.sql.functions as f


def union_dfs(chaseprime_df: DataFrame,  barclays_df: DataFrame) -> DataFrame:
    # Union the two DFs
    df = chaseprime_df.unionByName(barclays_df, allowMissingColumns=True)
    return df 

def monthly_totals(chaseprime_df: DataFrame,  barclays_df: DataFrame) -> DataFrame:
    """
    take both data frames and combine into 1 
    Then for each 
    """
    df = union_dfs(chaseprime_df, barclays_df)

    ccTotals = df\
        .withColumn("TRANSACTION_DATE", f.to_date("TRANSACTION_DATE", "MM/dd/yyyy"))\
        .withColumn("TRANSACTION_MONTH", f.trunc("TRANSACTION_DATE", "month"))\
        .withColumn("AMOUNT", f.col("AMOUNT").cast('double'))\
        .filter(~f.lower(f.col("DESCRIPTION")).like("%payment%"))\
        .groupBy("TRANSACTION_MONTH")\
        .sum("AMOUNT")\
        .withColumnRenamed("sum(AMOUNT)", "amount_total")\
        .orderBy("TRANSACTION_MONTH")

    ccTotals = ccTotals\
        .withColumn("amount_total", f.round(ccTotals.amount_total, 2))

    return ccTotals