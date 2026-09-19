from pyspark.sql import SparkSession, functions as F
from config import CANDIDATE_GROUPS, EDGES_PATH, MIN_CO_REVIEW_GROUPS

spark = SparkSession.builder.appName("AmazonReview-BuildGraph").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

groups = spark.read.parquet(CANDIDATE_GROUPS).select(
    "candidate_group_id", "product_id", "group_start", "group_end", "users"
)

# Explode users into one row per account in each candidate group.
members = groups.select(
    "candidate_group_id",
    "product_id",
    F.explode("users").alias("user_id")
)

# Self-join each candidate group to produce unordered account pairs.
left = members.alias("a")
right = members.alias("b")

pairs = (
    left.join(
        right,
        (F.col("a.candidate_group_id") == F.col("b.candidate_group_id")) &
        (F.col("a.user_id") < F.col("b.user_id")),
        "inner"
    )
    .select(
        F.col("a.user_id").alias("src"),
        F.col("b.user_id").alias("dst"),
        F.col("a.candidate_group_id").alias("candidate_group_id"),
        F.col("a.product_id").alias("product_id")
    )
)

edges = (
    pairs.groupBy("src", "dst")
    .agg(
        F.countDistinct("candidate_group_id").alias("co_review_group_count"),
        F.countDistinct("product_id").alias("distinct_products")
    )
    .filter(F.col("co_review_group_count") >= MIN_CO_REVIEW_GROUPS)
    .withColumn("weight", F.col("co_review_group_count").cast("double"))
)

(
    edges.write
    .mode("overwrite")
    .parquet(EDGES_PATH)
)

print(f"Wrote graph edges to {EDGES_PATH}")
spark.stop()
