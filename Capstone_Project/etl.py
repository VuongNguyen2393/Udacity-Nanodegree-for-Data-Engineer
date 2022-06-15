# Write code here
import os
import logging
import configparser
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf, col, expr, monotonically_increasing_id, to_timestamp, upper
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql import SparkSession

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']



def create_spark_session():
    """ Create spark session to process data """
    spark = SparkSession.builder\
                        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
                        .getOrCreate()
    return spark


def process_immigration_data(spark, input_data, output_data):
    """ Ingest immigration data then transform and loading into fact and dimension tables
    ---------------------------------
    Args:
        spark: Spark Session object
        input_data: Source data endpoint
        output_data: Destination location endpoint
    Return:
        None
     """
    logging.info("Reading data immigration from s3")

    df_immi =spark.read.load(input_data)

    logging.info("Loading data into fact_immigration")

    # extract columns to fact_immigration
    fact_immigration = df_immi.withColumn('stay_duration', expr('depdate - arrdate'))\
        .selectExpr('cicid as cic_id',
                    'i94port as city_code',
                    'i94addr as state_code',
                    'arrdate as arrival_date',
                    'fltno as flight_number',
                    'visatype as visa_type',
                    'stay_duration')

    logging.info("Reading data from I94_SAS_Labels_Descriptions ")

    with open("I94_SAS_Labels_Descriptions.SAS") as f:
        contents = f.readlines()

    # create dictionary of city_code and city_name
    city_dict = {}
    for city in contents[303:962]:
        code = city.split("'")[1]
        city_name = city.split("'")[3].split(",")[0]
        city_dict[code] = city_name

    # create udf of transforming between city_code into city_name
    get_city_name = udf(
        lambda z: city_dict[z] if z in city_dict.keys() else None)

    logging.info("Create new column in fact_immigration")

    # create new columns by using above udf
    fact_immigration = fact_immigration\
        .withColumn('city_name', get_city_name(col('city_code')))\
        .drop_duplicates(subset=['cic_id'])

    # write fact_immigration parquet files partitioned by city_code
    fact_immigration.write.mode("overwrite").partitionBy('city_code')\
                    .parquet(path=output_data + 'fact_immigration')

    logging.info("Loading data into dim_arrival_date")

    # crate get_datetime udf to transform SAS date into datetime
    get_datetime = udf(lambda z: datetime(
        1960, 1, 1) + timedelta(days=int(z)) if z is not None else None, TimestampType())

    # extract SAS arrival_date from staging table
    dim_arrival_date = df_immi.selectExpr('arrdate as arrival_date')

    # create new column with datetime type
    dim_arrival_date = dim_arrival_date.withColumn(
        'arrival_datetime', get_datetime('arrival_date'))

    # wrangling datetime type into different units
    dim_arrival_date = dim_arrival_date.selectExpr('arrival_date',
                                                   'arrival_datetime',
                                                   'year(arrival_datetime) as year',
                                                   'month(arrival_datetime) as month',
                                                   'dayofmonth(arrival_datetime) as day'
                                                   ).drop_duplicates(subset=['arrival_date'])

    # write dim_arrival_date parquet file into s3
    dim_arrival_date.write.mode("overwrite").parquet(
        path=output_data + 'dim_arrival_date')

    logging.info("Loading data into dim_visa")

    # extract columns into dim_visa
    dim_visa = df_immi.selectExpr('visatype as visa_type',
                                  'i94visa as visa_purpose'
                                  ).drop_duplicates(subset=['visa_type'])

    # write dim_visa parquet file into s3
    dim_visa.write.mode("overwrite").parquet(path=output_data + 'dim_visa')

    logging.info("Loading data into dim_personal")

    # extract columns into dim_personal
    dim_personal = df_immi.selectExpr('cicid as cic_id',
                                      'i94cit as country_of_birth',
                                      'i94res as country_of_residence',
                                      'biryear as year_of_birth',
                                      'gender'
                                      ).drop_duplicates(subset=['cic_id'])

    # write dim_personal parquet file into s3
    dim_personal.write.mode("overwrite").parquet(
        path=output_data + 'dim_personal')

    logging.info("Loading data into dim_airline")

    # extract columns into dim_airline
    dim_airline = df_immi.selectExpr('fltno as flight_number',
                                     'airline as airline_brand'
                                     ).drop_duplicates(subset=['flight_number'])
    # write dim_airline parquet file into s3
    dim_airline.write.mode("overwrite").parquet(
        path=output_data + 'dim_airline')


def process_demographic_data(spark, input_data, output_data):
    """ Ingest demographic data then transform and loading into fact and dimension tables
    ---------------------------------
    Args:
        spark: Spark Session object
        input_data: Source data endpoint
        output_data: Destination location endpoint
    Return:
        None
     """
    
    df_demographic = spark.read.format('csv')\
        .options(header=True, delimiter=';')\
        .load(input_data)

    # extract columns into dim_location
    dim_location = df_demographic.selectExpr('upper(City) as city_name',
                                             'State as state_name',
                                             '`State Code` as state_code',
                                             '`Median Age` as median_age',
                                             '`Male Population` as male_population',
                                             '`Female Population` as female_population',
                                             '`Total Population` as total_population',
                                             '`Number of Veterans` as number_of_veterans',
                                             '`Foreign-born` as foreign_born',
                                             '`Average Household Size` as avg_household_size'
                                             ).drop_duplicates(subset=['city_name'])

    # write dim_location parquet file into s3
    dim_location.write.mode("overwrite").parquet(
        path=output_data + 'dim_location')


def main():
    logging.info("Data processing starting with create spark session")
    spark = create_spark_session()
    immigration_path = "./sas_data"
    demographic_path = "us-cities-demographics.csv"
    output_data = DEST_S3_BUCKET

    logging.info("Immigration data processing")
    process_immigration_data(spark, immigration_path, output_data)
    logging.info("Demographic data processing")
    process_demographic_data(spark, demographic_path, output_data)
    logging.info("Data processing completed")


if __name__ == "__main__":
    main()
