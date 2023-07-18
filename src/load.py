from pyspark.sql import DataFrame


def load(type: str, df: DataFrame, target: str):
    """
    Load Data to Database and Filesystem based on type
    :param type: Input Storage type (JDBC|CSV) Based on type data stored in MySQL or FileSystem
    :param df: Input Dataframe
    :param target:
    :return: Input target -For filesystem - Location where to store the data
            -For MySQL - table name
    """

    driver_name = "org.sqlite.JDBC"
    url = "jdbc:sqlite:/Users/Carballeira/sqlite/db/cctx.db"

    if type == "JDBC":

        df.write.format("jdbc")\
            .mode("append")\
            .option("url", url)\
            .option("dbtable", target)\
            .option("driver",driver_name)\
            .save()

        print(f"Data successfully loaded to SQLite Database !!")

    if type == "CSV":
        # Write data on filesystem
        df.write.format("CSV").mode("overwrite").options(header=True).save(target)
        print(f"Data successfully loaded to filesystem !!")