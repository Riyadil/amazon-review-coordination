from pyspark.sql import SparkSession, functions as F, types as T
from config import RAW_REVIEW_PATH, REVIEWS_PARQUET, MIN_REVIEW_LENGTH, nonempty_paths

spark = SparkSession.builder.appName("AmazonReview-IngestClean").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Amazon Reviews'23 review JSONL records generally use fields such as
# user_id, asin, rating, text, timestamp, verified_purchase and helpful_vote.
# The schema is intentionally permissive for optional fields.
schema = T.StructType([
    T.StructField("rating", T.DoubleType(), True),
    T.StructField("title", T.StringType(), True),
    T.StructField("text", T.StringType(), True),
    T.StructField("images", T.ArrayType(T.StringType()), True),
    T.StructField("asin", T.StringType(), True),
    T.StructField("parent_asin", T.StringType(), True),
    T.StructField("user_id", T.StringType(), True),
    T.StructField("timestamp", T.LongType(), True),
    T.StructField("helpful_vote", T.LongType(), True),
    T.StructField("verified_purchase", T.BooleanType(), True),
])

inputs = nonempty_paths(__import__("config").REVIEW_INPUTS) or [RAW_REVIEW_PATH]

df = spark.read.schema(schema).json(inputs)

clean = (
    df
    .withColumn("review_text", F.trim(F.coalesce(F.col("text"), F.lit(""))))
    .withColumn("review_title", F.trim(F.coalesce(F.col("title"), F.lit(""))))
    .withColumn("user_id", F.trim(F.col("user_id")))
    .withColumn("product_id", F.coalesce(F.col("asin"), F.col("parent_asin")))
    .withColumn(
        "review_ts",
        F.to_timestamp(F.from_unixtime(F.col("timestamp") / F.lit(1000)))
    )
    .withColumn("review_date", F.to_date("review_ts"))
    .withColumn("year", F.year("review_ts"))
    .withColumn("review_length", F.length("review_text"))
    .withColumn("helpful_vote", F.coalesce(F.col("helpful_vote"), F.lit(0)))
    .withColumn("verified_purchase", F.coalesce(F.col("verified_purchase"), F.lit(False)))
    .dropDuplicates(["user_id", "product_id", "timestamp", "review_text"])
    .filter(F.col("user_id").isNotNull())
    .filter(F.col("product_id").isNotNull())
    .filter(F.col("review_ts").isNotNull())
)

# Short reviews remain in the cleaned dataset. They are excluded only from
# the later MinHash text-analysis stage, exactly as specified in the proposal.
clean = clean.withColumn(
    "eligible_for_text_similarity",
    F.col("review_length") >= F.lit(MIN_REVIEW_LENGTH)
)

(
    clean
    .repartition("year")
    .write
    .mode("overwrite")
    .partitionBy("year")
    .parquet(REVIEWS_PARQUET)
)

print(f"Wrote cleaned reviews to {REVIEWS_PARQUET}")
spark.stop()
