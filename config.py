import os

# ---------- Research parameters ----------
TIME_WINDOW_MINUTES = int(os.getenv("TIME_WINDOW_MINUTES", "30"))
MIN_REVIEW_LENGTH = int(os.getenv("MIN_REVIEW_LENGTH", "30"))
MIN_CO_REVIEW_GROUPS = int(os.getenv("MIN_CO_REVIEW_GROUPS", "3"))
MAX_CANDIDATE_GROUP_SIZE = int(os.getenv("MAX_CANDIDATE_GROUP_SIZE", "100"))
TIME_BUCKET_MINUTES = int(os.getenv("TIME_BUCKET_MINUTES", "10"))

# Text analysis parameters.
# MinHash is intentionally applied only to reviews >= MIN_REVIEW_LENGTH.
MINHASH_NUM_HASH_TABLES = int(os.getenv("MINHASH_NUM_HASH_TABLES", "8"))
NGRAM = int(os.getenv("NGRAM", "3"))
TEXT_JACCARD_THRESHOLD = float(os.getenv("TEXT_JACCARD_THRESHOLD", "0.75"))

# ---------- Storage ----------
S3_BUCKET = os.getenv("S3_BUCKET", "REPLACE_ME")
BASE_PREFIX = os.getenv(
    "BASE_PREFIX",
    "amazon-review-project"
)

S3_ROOT = f"s3a://{S3_BUCKET}/{BASE_PREFIX}"
RAW_REVIEW_PATH = os.getenv("RAW_REVIEW_PATH", f"{S3_ROOT}/raw/reviews")
RAW_METADATA_PATH = os.getenv("RAW_METADATA_PATH", f"{S3_ROOT}/raw/metadata")
REVIEWS_PARQUET = f"{S3_ROOT}/parquet/reviews"
ENRICHED_PARQUET = f"{S3_ROOT}/parquet/enriched"
CANDIDATE_GROUPS = f"{S3_ROOT}/candidates/product_time_groups"
EDGES_PATH = f"{S3_ROOT}/graph/edges"
COMPONENTS_PATH = f"{S3_ROOT}/graph/components"
TEXT_SIMILARITY_PATH = f"{S3_ROOT}/results/text_similarity"
GROUP_FEATURES_PATH = f"{S3_ROOT}/results/group_features"
FINAL_RESULTS_PATH = f"{S3_ROOT}/results/final_candidate_groups"
RUNTIME_PATH = f"{S3_ROOT}/results/runtime"

# Four categories from the project proposal.
CATEGORIES = [
    "Video Games",
    "CDs & Vinyl",
    "Arts Crafts & Sewing",
    "Baby Products",
]

# Set these to exact local/S3 paths if the downloaded files do not follow
# your preferred naming convention.
REVIEW_INPUTS = os.getenv("REVIEW_INPUTS", "").split(",") if os.getenv("REVIEW_INPUTS") else []
METADATA_INPUTS = os.getenv("METADATA_INPUTS", "").split(",") if os.getenv("METADATA_INPUTS") else []


def nonempty_paths(paths):
    return [p.strip() for p in paths if p and p.strip()]
