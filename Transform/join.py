# Create a sample DataFrame
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Transformations").getOrCreate()

data = [("John", 25, "USA"), ("Mary", 31, "Canada"), ("David", 42, "UK")]
df = spark.createDataFrame(data, ["Name", "Age", "Country"])

# Create another sample DataFrame
data2 = [("John", "Developer"), ("Mary", "Manager"), ("David", "Engineer")]
df2 = spark.createDataFrame(data2, ["Name", "Occupation"])

# Join the two DataFrames on the Name column
joined_df = df.join(df2, on="Name")

joined_df.show()