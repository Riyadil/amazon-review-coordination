import json
import time
import sys
from datetime import datetime, timezone
from pyspark.sql import SparkSession, functions as F
from config import ENRICHED_PARQUET, RUNTIME_PATH

# Usage:
# spark-submit ... src/08_runtime_experiment.py [label]
# Example labels: single_worker, multi_worker
label = sys.argv[1] if len(sys.argv) > 1 else "unspecified"

spark = SparkSession.builder.appName(
    f"AmazonReview-Runtime-{label}"
).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

start = time.perf_counter()

df = spark.read.parquet(ENRICHED_PARQUET)

# Representative distributed workload:
# filter + groupBy + distinct + aggregation.
result = (
    df.select("year", "product_id", "user_id", "rating")
    .filter(F.col("product_id").isNotNull())
    .groupBy("year")
    .agg(
        F.count("*").alias("reviews"),
        F.countDistinct("product_id").alias("products"),
        F.countDistinct("user_id").alias("users"),
        F.avg("rating").alias("avg_rating")
    )
    .orderBy("year")
)

result.collect()

elapsed = time.perf_counter() - start

record = [{
    "label": label,
    "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    "elapsed_seconds": elapsed,
    "master": spark.sparkContext.master,
    "application_id": spark.sparkContext.applicationId
}]

spark.createDataFrame(record).write.mode("append").json(RUNTIME_PATH)

print(json.dumps(record, indent=2))
spark.stop()
