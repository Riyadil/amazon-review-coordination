from pyspark.sql import SparkSession, functions as F
from config import (
    ENRICHED_PARQUET, COMPONENTS_PATH, EDGES_PATH,
    TEXT_SIMILARITY_PATH, GROUP_FEATURES_PATH, FINAL_RESULTS_PATH
)

spark = SparkSession.builder.appName("AmazonReview-GroupFeatures").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

reviews = spark.read.parquet(ENRICHED_PARQUET)
components = spark.read.parquet(COMPONENTS_PATH).select(
    "id", "component"
)

# Attach each review to the connected component of its account.
r = reviews.join(
    components,
    reviews.user_id == components.id,
    "inner"
).drop("id")

group_stats = (
    r.groupBy("component")
    .agg(
        F.countDistinct("user_id").alias("group_size"),
        F.count("*").alias("review_count"),
        F.countDistinct("product_id").alias("distinct_products"),
        F.avg("rating").alias("group_mean_rating"),
        F.stddev("rating").alias("group_rating_stddev"),
        F.avg(F.col("helpful_vote").cast("double")).alias("avg_helpful_votes"),
        F.sum(F.when(F.col("verified_purchase"), 1).otherwise(0)).alias("verified_reviews"),
        F.count("*").alias("total_reviews"),
        F.avg("rating_deviation_from_catalog").alias("mean_catalog_rating_deviation"),
        F.stddev("rating_deviation_from_catalog").alias("catalog_deviation_stddev"),
        F.min("review_ts").alias("first_review"),
        F.max("review_ts").alias("last_review")
    )
    .withColumn(
        "verified_purchase_rate",
        F.col("verified_reviews") / F.col("total_reviews")
    )
    .withColumn(
        "time_span_days",
        (
            F.col("last_review").cast("long") -
            F.col("first_review").cast("long")
        ) / 86400.0
    )
)

# Edge density = observed edges / possible edges.
edges = spark.read.parquet(EDGES_PATH)

edge_vertices = (
    edges.select(F.col("src").alias("id"))
    .union(edges.select(F.col("dst").alias("id")))
    .distinct()
    .join(components, "id", "inner")
)

edge_component_stats = (
    edge_vertices.alias("v")
    .join(
        edges.alias("e"),
        (F.col("v.id") == F.col("e.src")) |
        (F.col("v.id") == F.col("e.dst")),
        "inner"
    )
    .select(F.col("v.component").alias("component"), "e.*")
    .groupBy("component")
    .agg(
        F.countDistinct(
            F.concat_ws("||", "src", "dst")
        ).alias("edge_count"),
        F.avg("weight").alias("avg_edge_weight"),
        F.max("weight").alias("max_edge_weight")
    )
)

features = (
    group_stats.join(edge_component_stats, "component", "left")
    .withColumn(
        "possible_edges",
        F.col("group_size") * (F.col("group_size") - 1) / 2.0
    )
    .withColumn(
        "edge_density",
        F.when(
            F.col("possible_edges") > 0,
            F.col("edge_count") / F.col("possible_edges")
        ).otherwise(F.lit(0.0))
    )
)

# Text similarity is summarized at component level.
text = spark.read.parquet(TEXT_SIMILARITY_PATH)

text_user_components = (
    text.select(F.col("user_a").alias("id"))
    .union(text.select(F.col("user_b").alias("id")))
    .distinct()
    .join(components, "id", "inner")
    .distinct()
)

text_component = (
    text.join(
        text_user_components.select(
            F.col("id").alias("user_a"),
            "component"
        ),
        "user_a",
        "inner"
    )
    .groupBy("component")
    .agg(
        F.avg("jaccard_similarity").alias("avg_text_similarity"),
        F.max("jaccard_similarity").alias("max_text_similarity"),
        F.count("*").alias("similar_review_pair_count")
    )
)

final = features.join(text_component, "component", "left").fillna({
    "edge_count": 0,
    "avg_edge_weight": 0.0,
    "max_edge_weight": 0.0,
    "edge_density": 0.0,
    "avg_text_similarity": 0.0,
    "max_text_similarity": 0.0,
    "similar_review_pair_count": 0
})

# A transparent multi-feature descriptive score is provided only as a
# convenience for sorting/inspection. It is NOT a fraud probability and
# should not be presented as ground truth.
# Each component is normalized against its observed maximum.
maxes = final.agg(
    F.max("group_size").alias("max_group_size"),
    F.max("edge_density").alias("max_density"),
    F.max("avg_edge_weight").alias("max_weight"),
    F.max("avg_text_similarity").alias("max_text"),
    F.max(F.abs(F.col("mean_catalog_rating_deviation"))).alias("max_dev")
).collect()[0]

def safe_div(col, value):
    return F.when(F.lit(value).isNull() | (F.lit(value) == 0), F.lit(0.0)).otherwise(col / F.lit(value))

final = final.withColumn(
    "descriptive_feature_index",
    (
        safe_div(F.col("group_size").cast("double"), maxes["max_group_size"]) +
        safe_div(F.col("edge_density"), maxes["max_density"]) +
        safe_div(F.col("avg_edge_weight"), maxes["max_weight"]) +
        safe_div(F.col("avg_text_similarity"), maxes["max_text"]) +
        safe_div(F.abs(F.col("mean_catalog_rating_deviation")), maxes["max_dev"])
    ) / 5.0
)

(
    final.write
    .mode("overwrite")
    .parquet(GROUP_FEATURES_PATH)
)

(
    final.orderBy(F.desc("descriptive_feature_index"))
    .write
    .mode("overwrite")
    .parquet(FINAL_RESULTS_PATH)
)

print(f"Wrote group features to {GROUP_FEATURES_PATH}")
print(f"Wrote final candidate-group table to {FINAL_RESULTS_PATH}")
spark.stop()
