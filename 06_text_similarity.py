from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import NGram, HashingTF, MinHashLSH
from config import (
    ENRICHED_PARQUET, TEXT_SIMILARITY_PATH,
    MIN_REVIEW_LENGTH, NGRAM, MINHASH_NUM_HASH_TABLES,
    TEXT_JACCARD_THRESHOLD
)

spark = SparkSession.builder.appName("AmazonReview-MinHash").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(ENRICHED_PARQUET).select(
    "user_id", "product_id", "review_ts", "review_text", "review_length"
).filter(
    F.col("review_length") >= MIN_REVIEW_LENGTH
).filter(
    F.length("review_text") > 0
)

# Tokenization without external NLP dependencies.
tokens = (
    df.withColumn(
        "tokens",
        F.split(
            F.lower(
                F.regexp_replace("review_text", r"[^\\p{L}\\p{N}]+", " ")
            ),
            r"\\s+"
        )
    )
)

ngram = NGram(n=NGRAM, inputCol="tokens", outputCol="ngrams")
ngram_df = ngram.transform(tokens)

hashing = HashingTF(
    inputCol="ngrams",
    outputCol="features",
    numFeatures=1 << 18,
    binary=True
)

vector_df = hashing.transform(ngram_df).select(
    "user_id", "product_id", "review_ts", "review_text", "features"
)

mh = MinHashLSH(
    inputCol="features",
    outputCol="hashes",
    numHashTables=MINHASH_NUM_HASH_TABLES
)

model = mh.fit(vector_df)

# Approximate self-join finds candidate similar review texts without an
# all-pairs comparison. Distance is Jaccard distance for binary vectors.
similar = (
    model.approxSimilarityJoin(
        vector_df.alias("a"),
        vector_df.alias("b"),
        1.0 - TEXT_JACCARD_THRESHOLD,
        distCol="jaccard_distance"
    )
    .select(
        F.col("datasetA.user_id").alias("user_a"),
        F.col("datasetB.user_id").alias("user_b"),
        F.col("datasetA.product_id").alias("product_a"),
        F.col("datasetB.product_id").alias("product_b"),
        F.col("datasetA.review_ts").alias("time_a"),
        F.col("datasetB.review_ts").alias("time_b"),
        F.col("jaccard_distance")
    )
    .filter(F.col("user_a") < F.col("user_b"))
    .withColumn("jaccard_similarity", 1.0 - F.col("jaccard_distance"))
)

(
    similar.write
    .mode("overwrite")
    .parquet(TEXT_SIMILARITY_PATH)
)

print(f"Wrote MinHashLSH similarity results to {TEXT_SIMILARITY_PATH}")
spark.stop()
