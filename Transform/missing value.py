# Create a sample DataFrame
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Transformations").getOrCreate()

data = [("John", 25, "USA"), ("Mary", 31, "Canada"), ("David", 42, "UK")]
df = spark.createDataFrame(data, ["Name", "Age", "Country"])

# Replace missing values in the Age column with 0
from pyspark.sql.functions import coalesce
filled_df = df.withColumn("Age", coalesce(df["Age"], 0))

filled_df.show()