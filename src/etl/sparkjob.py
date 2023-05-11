import os
import logging
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, row_number, date_format
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType
from pyspark.sql.window import Window
from pyspark import SparkConf
import os
import uuid


# define the schema for incoming data
SCHEMA = StructType(
[
    StructField('id', StringType(), True), 
    StructField('name', StringType(), True),
    StructField('age', StringType(), True), 
]
)

def add_fields(df):
    # Add batch_id and current timestamp columns
    df = df.withColumn("timestamp", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("batch_uuid", lit(uuid.uuid4().hex))
    return df
    
def start_spark_history_server(log_dir,event_dir):
    """
    Launches a Spark History Server and configures it to read logs from the specified directory.
    """
    # Set Spark configuration
    conf = SparkConf().setAppName("SparkHistoryServer").set("spark.eventLog.enabled", "true") \
                      .set("spark.eventLog.dir", event_dir).set("spark.history.fs.logDirectory", log_dir)

    # Create a SparkSession
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    # Start Spark History Server
    os.system(f"nohup spark-submit --class org.apache.spark.deploy.history.HistoryServer \
            $SPARK_HOME/jars/spark-*.jar > /dev/null 2>&1 &")

    return spark
    
class IngestionJob:
    def __init__(self, spark, log_file):
        self.spark = spark

        # Initialize logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # Set up file handler
        file_handler = logging.FileHandler(log_file)

        # Set log message format
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        file_handler.setFormatter(formatter)

        # Add file handler to logger
        self.logger.addHandler(file_handler)
        
    def read_csv(self, SCHEMA, file_path):

    # read each csv file into a dataframe. We will consider all files are without header
        df = self.spark\
        .read.format("csv")\
        .option("delimiter", "|")\
        .option("header", "false")\
        .option("encoding", "ISO-8859-1")\
        .schema(SCHEMA)\
        .load(file_path)

    # now if any files happens to have a header then we can just remove that header line
        row1 = [i for i in df.head(1)[0].asDict().values()] # get first row
        schema_list = [(x.name) for x in SCHEMA.fields] # get schema as list
        
        if row1 == schema_list: # if first row is the schema then remove that row
            row1 = df.limit(1)
            df = df.subtract(row1)   
        
        print(file_path)
        self.logger.info(f"Read CSV file with {df.count()} rows from {file_path}")

        return df
        

    

    def ingest_data(self, df, output_path):
        
        # Write to Delta Lake with append mode and partition by batch_id and timestamp
        output_table = f"{output_path}"
        df.write.format("delta").mode("append").save(output_table)
        self.logger.info(f"Added batch_id and timestamp columns to DataFrame")
        return df
        
    def ingest_csv_to_deltalake(self, file_path, output_path):
    # define the schema for incoming data
        SCHEMA = StructType(
        [
            StructField('id', StringType(), True), 
            StructField('name', StringType(), True),
            StructField('age', StringType(), True), 
        ]
        )
    # read each csv file into a dataframe. We will consider all files are without header
        df = self.spark\
        .read.format("csv")\
        .option("delimiter", "|")\
        .option("header", "false")\
        .option("encoding", "ISO-8859-1")\
        .schema(SCHEMA)\
        .load(file_path)
    
    # now if any files happens to have a header then we can just remove that header line
        row1 = [i for i in df.head(1)[0].asDict().values()] # get first row
        schema_list = [(x.name) for x in SCHEMA.fields] # get schema as list
        
        if row1 == schema_list: # if first row is the schema then remove that row
            row1 = df.limit(1)
            df = df.subtract(row1)
            
        print(file_path)
        self.logger.info(f"Read CSV file with {df.count()} rows from {file_path}")

        # Add batch_id and current timestamp columns
        df = df.withColumn("timestamp", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        df = df.withColumn("batch_uuid", lit(uuid.uuid4().hex))
        self.logger.info(f"Added batch_id and timestamp columns to DataFrame")
        
        

        # Write to Delta Lake with append mode and partition by batch_id and timestamp
        output_table = f"{output_path}"
        df.write.format("delta").mode("append").save(output_table)
        self.logger.info(f"Wrote {df.count()} rows to Delta Lake at {output_table}")

if __name__ == "__main__":
    # Initialize SparkSession
    spark = SparkSession.builder.appName("IngestionJob").getOrCreate()

    # Parse arguments
    parser = argparse.ArgumentParser(description='Ingest CSV files into Delta Lake')
    parser.add_argument("--data_path", help="Path to csv files", required=True)
    parser.add_argument('--output_path', type=str, default='delta', help='Output path for Delta Lake table')
    parser.add_argument('--log_file', type=str, default='ingestion.log', help='Log file path')
    parser.add_argument('--event_dir', type=str, default='/events', help='Event directory')
    args = parser.parse_args()

    # create empty dataframe
    # Create an empty RDD
    emp_RDD = spark.sparkContext.emptyRDD()
     
    # Create empty schema
    columns = StructType([])
     
    # Create an empty RDD with empty schema
    final_df = spark.createDataFrame(data = emp_RDD,
                                 schema = SCHEMA)
    # Initialize IngestionJob
    job = IngestionJob(spark, args.log_file)

    files = os.listdir(args.data_path)
    print(files)
    file_paths = [file for file in files if file.endswith('.csv')]
    # Process each CSV file
    for file_path in file_paths:
        df = job.read_csv(SCHEMA, args.data_path + "/" + file_path)
        final_df = final_df.union(df)
    
    
    final_df = add_fields(final_df)
    print(final_df.printSchema())
    # job.ingest_csv_to_deltalake(args.data_path + "/" + file_path, args.output_path)
    job.ingest_data(final_df, args.output_path)

    # Stop SparkSession
    spark.stop()
    