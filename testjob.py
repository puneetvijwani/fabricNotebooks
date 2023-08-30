from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Stream CSV to Delta Table") \
    .getOrCreate()

# Define Schema
userSchema = StructType().add("name", "string").add("sales", "integer")

# Read from CSV as stream
streamingDF = spark.readStream \
    .schema(userSchema) \
    .option("maxFilesPerTrigger", 1) \
    .csv("Files/Streaming/")  # Replace with the actual path to your streaming CSV files

# Write to Delta table as stream
query = streamingDF.writeStream \
    .trigger(processingTime='5 seconds') \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", "Tables/Streaming_Table_test/_checkpoint") \
    .start("Tables/Streaming_Table_test")  # Replace with the path where you want to save the Delta table

# Await termination of the query
query.awaitTermination()

