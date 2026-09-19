from pyspark.sql import SparkSession, functions as F, types as T
from config import (
    REVIEWS_PARQUET, RAW_METADATA_PATH, ENRICHED_PARQUET,
    nonempty_paths
)

spark = SparkSession.builder.appName("AmazonReview-MetadataJoin").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

reviews = spark.read.parquet(REVIEWS_PARQUET)

# Amazon metadata contains a larger set of optional fields. We select the
# fields needed by the project after permissive JSON ingestion.
metadata = spark.read.json(
    nonempty_paths(__import__("config").METADATA_INPUTS) or [RAW_METADATA_PATH]
)

# Normalize possible identifier names.
if "parent_asin" in metadata.columns:
    metadata = metadata.withColumn(
        "product_id",
        F.coalesce(F.col("parent_asin"), F.col("asin"))
    )
else:
    metadata = metadata.withColumn("product_id", F.col("asin"))

# Normalize catalog rating if present.
if "average_rating" in metadata.columns:
    metadata = metadata.withColumn(
        "catalog_average_rating",
        F.col("average_rating").cast("double")
    )
elif "rating" in metadata.columns:
    metadata = metadata.withColumn(
        "catalog_average_rating",
        F.col("rating").cast("double")
    )
else:
    metadata = metadata.withColumn(
        "catalog_average_rating", F.lit(None).cast("double")
    )

# Keep common product metadata where available.
wanted = ["product_id", "catalog_average_rating"]
for c in ["price", "store", "brand", "title", "rating_number"]:
    if c in metadata.columns:
        wanted.append(c)

meta = metadata.select(*wanted).dropDuplicates(["product_id"])

enriched = (
    reviews.join(meta, on="product_id", how="left")
    .withColumn(
        "rating_deviation_from_catalog",
        F.when(
            F.col("catalog_average_rating").isNotNull(),
            F.col("rating") - F.col("catalog_average_rating")
        )
    )
)

(
    enriched
    .repartition("year")
    .write
    .mode("overwrite")
    .partitionBy("year")
    .parquet(ENRICHED_PARQUET)
)

print(f"Wrote enriched reviews to {ENRICHED_PARQUET}")
spark.stop()
