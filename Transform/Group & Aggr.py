# Create a sample DataFrame
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Transformations").getOrCreate()

data = [("John", 25, "USA"), ("Mary", 31, "Canada"), ("David", 42, "UK")]
df = spark.createDataFrame(data, ["Name", "Age", "Country"])

# Group the DataFrame by Country and calculate the average Age
grouped_df = df.groupBy("Country").avg("Age")

grouped_df.show()