# Create a sample DataFrame
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Transformations").getOrCreate()

data = [("John", 25, "USA"), ("Mary", 31, "Canada"), ("David", 42, "UK")]
df = spark.createDataFrame(data, ["Name", "Age", "Country"])

# Add a new column called Eligible with a value of True if Age is greater than 18
from pyspark.sql.functions import when
new_df = df.withColumn("Eligible", when(df["Age"] > 18, True).otherwise(False))

new_df.show()