# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a SparkSession
spark = SparkSession.builder.appName("SFTP Data Reader").getOrCreate()

# Define SFTP connection details
sftp_host = "your_sftp_host"
sftp_username = "your_sftp_username"
sftp_password = "your_sftp_password"
sftp_port = 22  # default SFTP port
sftp_file_path = "/path/to/your/file.csv"

# Read data from SFTP server using Spark's built-in SFTP connector
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ",") \
    .option("host", sftp_host) \
    .option("username", sftp_username) \
    .option("password", sftp_password) \
    .option("port", sftp_port) \
    .load(sftp_file_path)

# Print the schema of the DataFrame
print(df.schema)

# Show the first few rows of the DataFrame
df.show()

# Stop the SparkSession
spark.stop()