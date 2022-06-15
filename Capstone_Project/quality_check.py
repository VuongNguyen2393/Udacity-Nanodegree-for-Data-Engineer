# Write code here
import os
import logging
import configparser
from pyspark.sql import SparkSession


# AWS configuration
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ Create spark session to process data """
    spark = SparkSession.builder\
                        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
                        .getOrCreate()
    return spark


def check_records(spark, table_name, s3_bucket,primary_key):
    """ Check the number of record in each table
    ----------------------------
    Args:
        spark: Spark Session
        table_name: name of table
        s3_bucket: s3 bucket endpoint

    Return:
        None
     """
    s3_source = Path(f"{s3_bucket}{table_name}")

    for s3_dir in s3_source.iterdir():
        if s3_dir.is_dir():
            df = spark.read.parquet(str(s3_dir))
            record_num = df.count()
            if record_num <= 0:
                raise ValueError(
                    f"{table_name} table is empty - quality check fail!")
            elif record_num > df.select(primary_key).distinct().count():
                raise ValueError(
                    f"{table_name} table duplicates the unique key {primary_key} - quality check fail!")
            elif df.where(col(primary_key).isNull()).count() != 0:
                raise ValueError(
                    f"{table_name} has missing values in {primary_key} - quality check fail!")
            else:
                print(f"{table_name} have {record_num} rows without duplication - quality check pass!")


def main():
    logging.info("Data quality check starting with create spark session")
    spark = create_spark_session()
    s3_bucket = "s3a://vuongnd9-udacity-output/"
    table_list = ['fact_immigration', 'dim_visa', 'dim_arrival_date', 'dim_airline', 'dim_location', 'dim_personal']
    primary_key_list = ['cic_id','visa_type','arrival_date', 'flight_number','city_name','cic_id']
    for table, primary_key in zip(table_list, primary_key_list):
        check_records(spark, table, s3_bucket,primary_key)

if __name__ == "__main__":
    main()
