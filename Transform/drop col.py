# Create a sample DataFrame
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Transformations").getOrCreate()

data = [("John", 25, "USA"), ("Mary", 31, "Canada"), ("David", 42, "UK")]
df = spark.createDataFrame(data, ["Name", "Age", "Country"])

# Drop the Country column
dropped_df = df.drop("Country")

dropped_df.show()