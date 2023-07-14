from pyspark.sql import SparkSession

from extract import extract
from metadata.constant import CHASE_PRIME_SCHEMA, BARCLAYS_SCHEMA
#from transform import rename_cols, join_df, specific_cols
#from load import load

SPARK = SparkSession\
        .builder\
        .appName("moneytracker")\
        .getOrCreate()

#### Extract ####
chase_file_location = "./data/cc-transactions/Chase7437_Activity20210713_20230713_20230713.CSV"
barclays_file_location = "./data/Barclays/CreditCard_20230101_20230711.csv"

chaseprime_df = extract(SPARK, CHASE_PRIME_SCHEMA, chase_file_location)
barclays_df = extract(SPARK, BARCLAYS_SCHEMA, barclays_file_location)

#### Transformation ####

# 1. Rename Columns
# city_df = rename_cols(city_df, CITY_COL_DICT)
# country_df = rename_cols(country_df, COUNTRY_COL_DICT)
# country_language_df = rename_cols(country_language_df, COUNTRY_LANGUAGE_COL_DICT)

# # 2. Join DF with common column "country_code"
# country_city_df = join_df(country_df, city_df, JOIN_ON_COLUMNS, JOIN_TYPE)
# country_city_language_df = join_df(country_city_df, country_language_df, JOIN_ON_COLUMNS, JOIN_TYPE)

# # 3. Get specific cols
# country_city_language_df = specific_cols(country_city_language_df, SPEC_COLS)

# #### Load Data ####

# # MySQL 
# load("JDBC", country_city_language_df, "CountryCityLanguage")

# # FileSystem
# load("CSV", country_city_language_df, "output/countrycitylanguage.csv")