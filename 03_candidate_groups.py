from pyspark.sql import SparkSession, functions as F, Window
from config import (
    ENRICHED_PARQUET, CANDIDATE_GROUPS,
    TIME_WINDOW_MINUTES, TIME_BUCKET_MINUTES,
    MAX_CANDIDATE_GROUP_SIZE
)

spark = SparkSession.builder.appName("AmazonReview-CandidateGroups").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(ENRICHED_PARQUET).select(
    "user_id", "product_id", "review_ts", "rating",
    "review_text", "verified_purchase", "helpful_vote",
    "year"
).filter(
    F.col("user_id").isNotNull() &
    F.col("product_id").isNotNull() &
    F.col("review_ts").isNotNull()
)

# Spark window operation: within each product, order reviews by timestamp.
w = Window.partitionBy("product_id").orderBy(F.col("review_ts").cast("long"))

with_prev = df.withColumn(
    "previous_review_ts",
    F.lag("review_ts").over(w)
)

# Start a new temporal session when the gap is greater than the configured
# window. A cumulative sum creates session/group identifiers.
with_break = with_prev.withColumn(
    "new_session",
    F.when(
        F.col("previous_review_ts").isNull() |
        (
            F.col("review_ts").cast("long") -
            F.col("previous_review_ts").cast("long")
        ) > TIME_WINDOW_MINUTES * 60,
        1
    ).otherwise(0)
)

w2 = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)

sessions = with_break.withColumn(
    "session_number",
    F.sum("new_session").over(w2)
)

groups = (
    sessions
    .groupBy("product_id", "session_number")
    .agg(
        F.min("review_ts").alias("group_start"),
        F.max("review_ts").alias("group_end"),
        F.countDistinct("user_id").alias("group_size"),
        F.collect_set("user_id").alias("users")
    )
    .withColumn(
        "duration_minutes",
        (
            F.col("group_end").cast("long") -
            F.col("group_start").cast("long")
        ) / 60.0
    )
)

# Oversized groups are split into fixed time buckets so a highly popular
# product does not create one enormous all-to-all account-pair operation.
expanded = sessions.join(
    groups.select("product_id", "session_number", "group_size"),
    ["product_id", "session_number"],
    "inner"
)

expanded = expanded.withColumn(
    "bucket_number",
    F.floor(
        (
            F.col("review_ts").cast("long") -
            F.unix_timestamp(F.col("review_ts").cast("timestamp"))
        ) / (TIME_BUCKET_MINUTES * 60)
    )
)

# The expression above intentionally uses a deterministic time-bucket key
# relative to the review timestamp. For oversized sessions, use epoch-minute
# buckets directly.
expanded = expanded.withColumn(
    "time_bucket",
    F.floor(F.col("review_ts").cast("long") / (TIME_BUCKET_MINUTES * 60))
)

candidate = (
    expanded
    .groupBy("product_id", "session_number", "time_bucket")
    .agg(
        F.min("review_ts").alias("group_start"),
        F.max("review_ts").alias("group_end"),
        F.countDistinct("user_id").alias("group_size"),
        F.collect_set("user_id").alias("users")
    )
    .filter(F.col("group_size") >= 2)
    .filter(F.col("group_size") <= MAX_CANDIDATE_GROUP_SIZE)
    .withColumn(
        "candidate_group_id",
        F.sha2(
            F.concat_ws(
                "||",
                "product_id",
                F.col("session_number").cast("string"),
                F.col("time_bucket").cast("string")
            ),
            256
        )
    )
)

(
    candidate.write
    .mode("overwrite")
    .parquet(CANDIDATE_GROUPS)
)

print(f"Wrote candidate groups to {CANDIDATE_GROUPS}")
spark.stop()
