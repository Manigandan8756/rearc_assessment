from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
from delta import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

spark=None

def initiate_spark_session():
    """
    To instantiate the Spark session object with
    required libs to read and manipulate data from S3
    """
    # Create a Spark session
    # Provide valid AWS Access Key Id and Value
    global spark 
    builder = SparkSession.builder \
        .appName("Metadata Read") \
        .master('local') \
        .config("spark.hadoop.fs.s3a.access.key", "<Access_Key_Id>") \
        .config("spark.hadoop.fs.s3a.secret.key", "<Access_Key>") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") 
    my_packages = ["org.apache.hadoop:hadoop-aws:3.3.1"]

    spark = configure_spark_with_delta_pip(builder, extra_packages=my_packages).getOrCreate()
    print("spark version::", spark.version)


def read_bls_data():
    """
    To read the BLS data from S3, pre-process and 
    return the transformed dataframe
    """
    # S3 bucket and file path
    s3_bucket = "rearc-assessment"
    s3_file_path = f"s3a://{s3_bucket}/bls/pr.data.0.Current"

    # Read BLS data CSV file from S3
    df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", "\t") \
        .load(s3_file_path)
    
    # Trim leading and tralining spaces in column names and values
    trimmed_columns = [col.strip() for col in df.columns]  # Use strip() to trim spaces
    trimmed_df = df.toDF(*trimmed_columns)
    trimmed_df=trimmed_df.select([trim(col).alias(col) for col in trimmed_df.columns])

    qtr_window = Window.partitionBy("series_id", "year").orderBy(desc("value"))
    year_window = Window.partitionBy("series_id").orderBy(desc("value"))

    final_df = trimmed_df.withColumn("qtr_row_no", row_number().over(qtr_window)).filter(col("qtr_row_no") == 1)\
        .withColumn("year_row_no", row_number().over(year_window)).filter(col("year_row_no") == 1)
    # Sort and Display the DataFrame schema and data
    final_df.orderBy(col("series_id").asc()).drop("year_row_no", "qtr_row_no", "period", "footnote_codes").show(truncate=False)
    return df # Return the source dataframe for Part 3.c

def read_usa_population_data():
    """
    To read the USA population data from S3, pre-process and 
    return the transformed dataframe
    """
    # S3 bucket and file path
    s3_bucket = "rearc-assessment"
    s3_file_path = f"s3a://{s3_bucket}/usa_data/usa_population_data.json"

    # USA data population schema
    data_schema = StructType([
        StructField("ID Nation", StringType(), True),
        StructField("Nation", StringType(), True),
        StructField("ID Year", IntegerType(), True),
        StructField("Year", StringType(), True),
        StructField("Population", LongType(), True),
        StructField("Slug Nation", StringType(), True)
    ])

    # Read USA data population JSON file from S3
    df = spark.read.schema(data_schema).option("multiline", "true")\
        .option("encoding", "UTF-8").json(s3_file_path)
    
    # Filter the DataFrame for years 2013 to 2018. Since the records are available from 2013.
    filtered_df = df.filter((col("Year") >= "2013") & (col("Year") <= "2018"))

    # Calculate mean and standard deviation of the population
    stats_df = filtered_df.agg(
        mean(col("Population")).alias("Mean Population"),
        stddev(col("Population")).alias("Standard Deviation")
    )
    stats_df.show(truncate=False)
    return df # Return the source dataframe for Part 3.c



def join_bls_pop_data(bls_df, pop_df):
    """
    To join the BLS and Population data to derive the final output
    """
    join_df = bls_df.filter(col("year") >=2013).join(pop_df, bls_df["year"] == pop_df["year"], "left")\
        .select(bls_df["*"], pop_df["Population"]).drop("footnote_codes").show(truncate=False)
    return join_df


if __name__ == "__main__":
    initiate_spark_session()
    bls_df = read_bls_data()
    pop_df = read_usa_population_data()
    join_bls_pop_data(bls_df, pop_df)