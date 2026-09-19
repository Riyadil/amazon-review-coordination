from pyspark.sql import SparkSession, functions as F
from graphframes import GraphFrame
from config import EDGES_PATH, COMPONENTS_PATH, CANDIDATE_GROUPS

spark = SparkSession.builder.appName("AmazonReview-GraphFrames").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

edges = spark.read.parquet(EDGES_PATH).select(
    "src", "dst", "weight", "co_review_group_count", "distinct_products"
)

# GraphFrames expects a vertex table with an "id" column.
vertices = (
    edges.select(F.col("src").alias("id"))
    .union(edges.select(F.col("dst").alias("id")))
    .distinct()
)

graph = GraphFrame(vertices, edges)

components = graph.connectedComponents()

(
    components.write
    .mode("overwrite")
    .parquet(COMPONENTS_PATH)
)

# A compact component membership table is also useful downstream.
(
    components
    .groupBy("component")
    .agg(F.countDistinct("id").alias("group_size"))
    .filter(F.col("group_size") >= 2)
    .write.mode("overwrite")
    .parquet(f"{COMPONENTS_PATH}/summary")
)

print(f"Wrote GraphFrames connected components to {COMPONENTS_PATH}")
spark.stop()
