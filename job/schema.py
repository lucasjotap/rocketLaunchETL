from pyspark.sql.types import StructType, StructField, LongType, StringType

# Define the schema
custom_schema = StructType([
    StructField("id", LongType(), nullable=True),
    StructField("image_url", StringType(), nullable=True),
    StructField("news_site", StringType(), nullable=True),
    StructField("published_at", StringType(), nullable=True),
    StructField("summary", StringType(), nullable=True),
    StructField("title", StringType(), nullable=True),
    StructField("updated_at", StringType(), nullable=True),
    StructField("url", StringType(), nullable=True),
])

