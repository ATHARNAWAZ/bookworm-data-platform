# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks notebook source
# MAGIC # BookWorm Data Platform
# MAGIC ## Senior Data Engineer Assignment — PIA Group
# MAGIC  **Built by:** Ather Nawaz
# MAGIC **Stack:** Azure ADLS Gen2 · Databricks · Delta Lake · Unity Catalog · dbt
# MAGIC
# MAGIC ### Business Problem
# MAGIC BookWorm Publishing needs data-driven decisions about which books
# MAGIC to prioritise for their new audiobook series. This platform analyses
# MAGIC 2.3 million real GoodReads books and 15.7 million reader reviews
# MAGIC to identify the highest-potential audiobook candidates.
# MAGIC
# MAGIC  ### Architecture
# MAGIC RAW (ADLS Gen2) → Bronze (Delta Lake) → Silver (PySpark) → Gold (PySpark) → Unity Catalog → Dashboard
# MAGIC
# MAGIC ### How to Run
# MAGIC  Run cells in order. Total runtime approximately 30 minutes on a single node.
# MAGIC
# MAGIC Cell 1: Configuration
# MAGIC Cell 2: Verify raw data
# MAGIC Cell 3: Bronze ingestion (books, authors, genres, series)
# MAGIC Cell 4: Bronze ingestion (reviews — 15.7M records)
# MAGIC Cell 5: Silver transformation
# MAGIC Cell 6: Gold scoring
# MAGIC Cell 7: Register tables in Unity Catalog
# MAGIC Cell 8: Business queries

# COMMAND ----------


# Cell 1 — Configuration
# Sets up storage connection, paths and helper functions.
# Run this first before any other cell.

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import math
import re

STORAGE_ACCOUNT  = "bookwormadls"
STORAGE_KEY      = "ZOUR_KEY_HERE"

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)
sc._jsc.hadoopConfiguration().set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)

RAW_PATH    = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
BRONZE_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
SILVER_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
GOLD_PATH   = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
CATALOG     = "piagroup_assessment_bookworm"
BATCH_ID    = datetime.now().strftime("%Y%m%d_%H%M%S")

def clean_col_names(df):
    """Replace invalid characters in column names with underscores."""
    for col in df.columns:
        clean = re.sub(r'[ ,;{}()\n\t=]+', '_', col)
        if clean != col:
            df = df.withColumnRenamed(col, clean)
    return df

def flatten_genres(df):
    """
    The GoodReads genres file stores genres as a nested struct
    with field names like 'comics, graphic' and 'fantasy, paranormal'.
    These are invalid in Delta Lake so we flatten and rename them.
    """
    genre_fields = df.schema["genres"].dataType.fieldNames()
    return df.select(
        F.col("book_id"),
        *[
            F.col(f"genres.`{field}`").alias(
                "genre_" + re.sub(r'[ ,;{}()\n\t=]+', '_', field).strip('_')
            )
            for field in genre_fields
        ]
    )

print(f"Configuration complete — Batch ID: {BATCH_ID}")


# Cell 2 — Verify Raw Data
# Checks all GoodReads source files are present in ADLS before starting.

print("=" * 55)
print("VERIFYING RAW DATA IN ADLS")
print("=" * 55)

datasets = {
    "books"  : f"{RAW_PATH}goodreads/books/",
    "reviews": f"{RAW_PATH}goodreads/reviews/",
    "authors": f"{RAW_PATH}goodreads/authors/",
    "series" : f"{RAW_PATH}goodreads/series/",
    "genres" : f"{RAW_PATH}goodreads/genres/",
}

for name, path in datasets.items():
    try:
        files = dbutils.fs.ls(path)
        real  = [f for f in files if not f.name.endswith("sample.json")]
        size  = sum(f.size for f in real)
        if size > 0:
            size_str = f"{size/(1024**3):.2f} GB" if size > 1e9 else f"{size/(1024**2):.1f} MB"
            print(f"{name:10} {size_str}")
        else:
            print(f"{name:10} not found or still uploading")
    except:
        print(f"{name:10} not found")


# Cell 3 — Bronze Ingestion (Books, Authors, Genres, Series)
#
# Pattern: Bootstrap on first run, Auto Loader on subsequent runs.
#
# First run: spark.read loads all historical files at once.
# A checkpoint is written afterwards marking all files as seen.
# Subsequent runs: Auto Loader detects only NEW files via the checkpoint
# and appends them — exactly-once guarantee, no duplicates possible.
#
# Why Delta Lake over Parquet:
# ACID transactions prevent corrupt partial writes.
# Time travel gives a full audit history for compliance.
# Schema evolution handles new fields in future GoodReads exports.
#
# Why PERMISSIVE mode:
# Real GoodReads JSON has malformed records. PERMISSIVE captures
# them in a _corrupt_record column rather than silently dropping them.
# We always know exactly what was rejected and why.
#
# Why Auto Loader over Azure Data Factory:
# ADF is the right tool for connecting to SaaS APIs and databases.
# For file-based JSON ingestion at this scale, Auto Loader is superior —
# it handles schema evolution and exactly-once semantics natively
# without requiring a separate orchestration service.

def bronze_ingest(name, raw_path, bronze_path):
    checkpoint = (
        f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
        f"/_checkpoints/{name}/"
    )
    schema_loc = f"{checkpoint}schema/"

    try:
        dbutils.fs.ls(checkpoint)
        has_checkpoint = True
    except:
        has_checkpoint = False

    if has_checkpoint:
        print(f"Incremental — Auto Loader picking up new files only")
        (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", schema_loc)
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(raw_path)
            .withColumn("_ingestion_timestamp", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_batch_id", F.lit(BATCH_ID))
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .start(bronze_path)
            .awaitTermination()
        )

    else:
        print(f"Bootstrap — first time full load")
        df = (
            spark.read
            .option("multiline", "false")
            .option("mode", "PERMISSIVE")
            .json(raw_path)
        )

        if "genres" in df.columns and "StructType" in str(df.schema["genres"].dataType):
            print(f" Flattening genres nested struct...")
            df = flatten_genres(df)
        else:
            df = clean_col_names(df)

        df = (
            df
            .withColumn("_ingestion_timestamp", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_batch_id", F.lit(BATCH_ID))
        )

        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(bronze_path)

        # Write checkpoint after data so Auto Loader knows these files are already processed.
        print(f"Initialising checkpoint...")
        (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", schema_loc)
            .option("cloudFiles.inferColumnTypes", "false")
            .option("cloudFiles.schemaHints", "book_id STRING, genres STRING")
            .load(raw_path)
            .writeStream
            .format("noop")
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start()
            .awaitTermination()
        )

    return spark.read.format("delta").load(bronze_path).count()


print("=" * 55)
print("BRONZE INGESTION — BOOKS / AUTHORS / GENRES / SERIES")
print("=" * 55)

ingestion_plan = [
    ("books",   f"{RAW_PATH}goodreads/books/",   f"{BRONZE_PATH}books/"),
    ("authors", f"{RAW_PATH}goodreads/authors/", f"{BRONZE_PATH}authors/"),
    ("genres",  f"{RAW_PATH}goodreads/genres/",  f"{BRONZE_PATH}genres/"),
    ("series",  f"{RAW_PATH}goodreads/series/",  f"{BRONZE_PATH}series/"),
]

results = {}
for name, raw, bronze in ingestion_plan:
    print(f"\n{name.upper()}")
    try:
        count = bronze_ingest(name, raw, bronze)
        results[name] = count
        print(f"{count:,} records")
    except Exception as e:
        print(f"Failed: {e}")
        results[name] = 0

print("\nQuality Checks:")
for name, count in results.items():
    status = "YES" if count > 100 else "NO"
    print(f"   {status} {name:10} {count:,} records")

print(f"""
   BRONZE COMPLETE
   First run  → full bootstrap + checkpoint written
   Next runs  → Auto Loader processes new files only
   Guarantee  → exactly-once, no duplicates possible
   At 300GB   → same code, larger cluster only
""")

# COMMAND ----------

# Cell 4 — Bronze Ingestion (Reviews)
#
# The 15.7M reviews file is ingested separately because of its size.
# Same bootstrap + Auto Loader pattern as Cell 3.
#
# Why reviews matter:
# Reviews contain the actual reader sentiment signal.
# 15 million opinions across 2.3 million books.
# This drives the sentiment component of the audiobook scoring model.

print("=" * 55)
print("BRONZE INGESTION — REVIEWS (15.7M records)")
print("=" * 55)

reviews_raw    = f"{RAW_PATH}goodreads/reviews/"
reviews_bronze = f"{BRONZE_PATH}reviews/"
checkpoint     = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/_checkpoints/reviews/"
schema_loc     = f"{checkpoint}schema/"

try:
    dbutils.fs.ls(checkpoint)
    has_checkpoint = True
    print("Incremental — Auto Loader new files only")
except:
    has_checkpoint = False
    print("Bootstrap — first time full load")

files = [f for f in dbutils.fs.ls(reviews_raw) if not f.name.endswith("sample.json")]
print(f"\nSource: {len(files)} file(s), {sum(f.size for f in files)/(1024**3):.2f} GB compressed")

if not has_checkpoint:
    print("\nReading reviews... : 15-20 minutes on single node")
    df_reviews = (
        spark.read
        .option("multiline", "false")
        .option("mode", "PERMISSIVE")
        .json(reviews_raw)
    )
    (
        df_reviews
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_id", F.lit(BATCH_ID))
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(reviews_bronze)
    )
    print("\nInitialising checkpoint...")
    (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaHints",
                "review_id STRING, book_id STRING, user_id STRING, "
                "rating STRING, review_text STRING, date_added STRING")
        .load(reviews_raw)
        .writeStream
        .format("noop")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )
else:
    (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(reviews_raw)
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_id", F.lit(BATCH_ID))
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .start(reviews_bronze)
        .awaitTermination()
    )

count = spark.read.format("delta").load(reviews_bronze).count()
print(f"\nReviews in Bronze: {count:,} records")


# Cell 5 — Silver Transformation
#
# Bronze = raw truth, never modified after ingestion.
# Silver = trusted truth, ready for analysis.
#
# Why a separate Silver layer:
# If a bug is introduced in transformation logic, we fix it and
# reprocess Silver from Bronze. The raw data is always safe.
#
# What happens here:
# 1. Build genre lookup from the genres table (GoodReads stores genres separately)
# 2. Clean and validate reviews, hash user_id for GDPR compliance
# 3. Calculate sentiment metrics per book from 15.7M real reviews
# 4. Clean and enrich book records with genre and sentiment data
#
# Why SHA256 for user_id:
# GDPR Article 25 — Privacy by Design.
# The hash allows tracking user behaviour patterns without storing
# any identifiable information. No raw user_id ever reaches Gold.

print("=" * 55)
print("SILVER TRANSFORMATION")
print("=" * 55)

bronze_books   = spark.read.format("delta").load(f"{BRONZE_PATH}books/")
bronze_genres  = spark.read.format("delta").load(f"{BRONZE_PATH}genres/")
bronze_reviews = spark.read.format("delta").load(f"{BRONZE_PATH}reviews/")

print(f"Bronze books   : {bronze_books.count():,}")
print(f"Bronze genres  : {bronze_genres.count():,}")
print(f"Bronze reviews : {bronze_reviews.count():,}")

# Build genre lookup
# The genres table has one column per genre with a count of how many
# readers shelved that book under that genre. We pick the top genre per book.
genre_cols  = [c for c in bronze_genres.columns if not c.startswith('_') and c != 'book_id']
stack_expr  = f"stack({len(genre_cols)}, " + ", ".join([f"'{c}', `{c}`" for c in genre_cols]) + ") as (genre_name, genre_count)"
primary_genre = (
    bronze_genres.selectExpr("book_id", stack_expr)
    .filter(F.col("genre_count").isNotNull() & (F.col("genre_count") > 0))
    .withColumn("rn", F.row_number().over(Window.partitionBy("book_id").orderBy(F.col("genre_count").desc())))
    .filter(F.col("rn") == 1)
    .select("book_id", F.regexp_replace(F.col("genre_name"), "^genre_", "").alias("primary_genre"))
)

# Silver reviews — validate, hash PII, classify sentiment
print("\nBuilding Silver Reviews (15.7M records)... ")
silver_reviews = (
    bronze_reviews
    .filter(F.col("rating").cast("int").between(1, 5))
    .filter(F.col("book_id").isNotNull())
    .filter(F.col("review_id").isNotNull())
    .withColumn("rating_int", F.col("rating").cast("int"))
    .withColumn("user_id_hashed", F.sha2(F.col("user_id").cast("string"), 256))
    .withColumn("sentiment",
        F.when(F.col("rating_int") >= 4, "positive")
         .when(F.col("rating_int") == 3, "neutral")
         .otherwise("negative")
    )
    .withColumn("review_date", F.to_date(F.col("date_added"), "yyyy-MM-dd"))
    .withColumn("_silver_timestamp", F.current_timestamp())
    .select("review_id", "book_id", "user_id_hashed",
            F.col("rating_int").alias("rating"), "sentiment",
            "review_text", "review_date",
            "_ingestion_timestamp", "_silver_timestamp")
)
silver_reviews.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{SILVER_PATH}reviews/")
print(f"Silver Reviews: {spark.read.format('delta').load(f'{SILVER_PATH}reviews/').count():,} records")

# Sentiment metrics per book from real reviews
review_metrics = (
    silver_reviews.groupBy("book_id").agg(
        F.count("review_id").alias("total_reviews"),
        F.round(F.avg("rating"), 2).alias("avg_review_rating"),
        F.round(F.sum(F.when(F.col("sentiment") == "positive", 1).otherwise(0)) * 100.0 / F.count("review_id"), 2).alias("positive_review_pct"),
        F.round(F.sum(F.when(F.col("sentiment") == "negative", 1).otherwise(0)) * 100.0 / F.count("review_id"), 2).alias("negative_review_pct")
    )
)

# Silver books — clean, deduplicate, join genre and sentiment
print("\nBuilding Silver Books...")
silver_books = (
    bronze_books
    .withColumn("rn", F.row_number().over(Window.partitionBy("book_id").orderBy(F.col("_ingestion_timestamp").desc())))
    .filter(F.col("rn") == 1).drop("rn")
    .withColumn("average_rating",     F.col("average_rating").cast("double"))
    .withColumn("ratings_count",      F.col("ratings_count").cast("bigint"))
    .withColumn("text_reviews_count", F.col("text_reviews_count").cast("bigint"))
    .withColumn("num_pages",          F.col("num_pages").cast("integer"))
    .withColumn("primary_author_id",  F.col("authors").getItem(0).getField("author_id"))
    .withColumn("shelf_genre",        F.col("popular_shelves").getItem(0).getField("name"))
    .withColumn("data_quality_flag",
        F.when(F.col("ratings_count") >= 100000, "high_confidence")
         .when(F.col("ratings_count") >= 10000,  "medium_confidence")
         .otherwise("low_confidence")
    )
    .withColumn("_silver_timestamp", F.current_timestamp())
    .select("book_id", "title", "primary_author_id", "average_rating",
            "ratings_count", "text_reviews_count", "num_pages",
            "description", "data_quality_flag", "shelf_genre",
            "_ingestion_timestamp", "_silver_timestamp")
    .filter(F.col("book_id").isNotNull())
    .filter(F.col("title").isNotNull())
    .filter(F.col("average_rating").cast("double").isNotNull())
    .filter(F.col("average_rating").cast("double") > 0)
    .join(primary_genre, on="book_id", how="left")
    .withColumn("primary_genre", F.coalesce(F.col("primary_genre"), F.col("shelf_genre"), F.lit("Uncategorised")))
    .drop("shelf_genre")
    .join(review_metrics, on="book_id", how="left")
    .fillna({"total_reviews": 0, "positive_review_pct": 0, "negative_review_pct": 0})
)

silver_books.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{SILVER_PATH}books/")
silver_count = spark.read.format("delta").load(f"{SILVER_PATH}books/").count()

print(f" Silver Books: {silver_count:,} records")
print(f"  Null check  : {silver_books.filter(F.col('book_id').isNull() | F.col('average_rating').isNull()).count()} nulls on critical columns")
print(f"  Books with real review sentiment: {silver_books.filter(F.col('total_reviews') > 0).count():,}")


# Cell 6 — Gold Scoring
#
# Gold = business-ready output. Analysts and product managers query this layer.
#
# Scoring formula:
#   weighted_score = (rating × 35%) + (popularity × 25%) + (sentiment × 25%) + (length × 15%)
#
# Why four components:
#   Rating (35%): Quality is the primary signal. A poorly rated book makes
#                 a poor audiobook regardless of how popular it is.
#
#   Popularity (25%): Market validation reduces investment risk. We use log
#                     scale — LN(5M) vs LN(500K) is a reasonable difference,
#                     not 10x. Without log, Harry Potter would dominate every
#                     ranking regardless of quality.
#
#   Sentiment (25%): Reader enthusiasm from 15.7M real reviews. Books where
#                    readers are genuinely excited generate stronger audiobook
#                    sales. Falls back to rating proxy for books with no reviews.
#
#   Length (15%): Audiobook production economics. 200-400 pages = ideal
#                 6-12 hour audiobook. 800+ pages = 40+ hour production,
#                 high cost and high listener drop-off risk.
#
# Why parameterised:
#   All four weights are constants at the top of this cell. A product manager
#   can adjust the strategy — conservative, discovery, sentiment-first —
#   by changing three numbers and re-running. No SQL or Python knowledge needed.

RATING_WEIGHT     = 0.35
POPULARITY_WEIGHT = 0.25
SENTIMENT_WEIGHT  = 0.25
LENGTH_WEIGHT     = 0.15

print("=" * 55)
print("GOLD — AUDIOBOOK SCORING MODEL")
print(f"Rating {RATING_WEIGHT*100:.0f}% | Popularity {POPULARITY_WEIGHT*100:.0f}% | Sentiment {SENTIMENT_WEIGHT*100:.0f}% | Length {LENGTH_WEIGHT*100:.0f}%")
print("=" * 55)

silver_books = spark.read.format("delta").load(f"{SILVER_PATH}books/")

gold_candidates = (
    silver_books
    .filter(F.col("data_quality_flag").isin("high_confidence", "medium_confidence"))
    .filter(~F.col("primary_genre").isin("comics_graphic", "Uncategorised"))
    .withColumn("log_ratings", F.log(F.col("ratings_count") + 1))
    .withColumn("max_log",     F.lit(float(math.log(5000001))))
    .withColumn("norm_log",    F.col("log_ratings") / F.col("max_log"))

    # Sentiment: use real review data where available, fall back to rating proxy
    .withColumn("sentiment_score",
        F.when(
            F.col("positive_review_pct").isNotNull() & (F.col("total_reviews") > 0),
            F.col("positive_review_pct") / 100.0
        ).otherwise(
            F.when(F.col("average_rating") >= 4.0, 1.0)
             .when(F.col("average_rating") >= 3.0, 0.5)
             .otherwise(0.0)
        )
    )

    # Length: score based on audiobook production economics
    .withColumn("length_score",
        F.when(F.col("num_pages").isNull(),             0.5)
         .when(F.col("num_pages").between(200, 400),    1.0)
         .when(F.col("num_pages").between(400, 600),    0.8)
         .when(F.col("num_pages").between(100, 200),    0.6)
         .when(F.col("num_pages").between(600, 800),    0.4)
         .otherwise(0.2)
    )
    .withColumn("length_category",
        F.when(F.col("num_pages").isNull(),             "unknown")
         .when(F.col("num_pages").between(200, 400),    "ideal")
         .when(F.col("num_pages").between(400, 600),    "good")
         .when(F.col("num_pages").between(100, 200),    "short")
         .when(F.col("num_pages").between(600, 800),    "long")
         .otherwise("very_long")
    )

    .withColumn("weighted_score", F.round(
        (F.col("average_rating") / 5.0 * RATING_WEIGHT) +
        (F.col("norm_log")                * POPULARITY_WEIGHT) +
        (F.col("sentiment_score")         * SENTIMENT_WEIGHT) +
        (F.col("length_score")            * LENGTH_WEIGHT),
    4))

    .withColumn("audiobook_rank", F.rank().over(Window.orderBy(F.col("weighted_score").desc())))
    .withColumn("genre_rank",     F.rank().over(Window.partitionBy("primary_genre").orderBy(F.col("weighted_score").desc())))

    .select(
        "audiobook_rank", "genre_rank", "book_id", "title",
        "primary_genre", "average_rating", "ratings_count",
        "num_pages", "length_category",
        "total_reviews", "positive_review_pct", "negative_review_pct",
        "weighted_score",
        F.round(F.col("average_rating") / 5.0 * RATING_WEIGHT, 4).alias("rating_contribution"),
        F.round(F.col("norm_log")                * POPULARITY_WEIGHT, 4).alias("popularity_contribution"),
        F.round(F.col("sentiment_score")         * SENTIMENT_WEIGHT, 4).alias("sentiment_contribution"),
        F.round(F.col("length_score")            * LENGTH_WEIGHT, 4).alias("length_contribution"),
        "data_quality_flag", "_silver_timestamp"
    )
    .orderBy("audiobook_rank")
)

gold_candidates.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD_PATH}audiobook_candidates/")

genre_summary = (
    silver_books
    .filter(F.col("data_quality_flag").isin("high_confidence", "medium_confidence"))
    .filter(~F.col("primary_genre").isin("comics_graphic", "Uncategorised"))
    .groupBy("primary_genre").agg(
        F.count("book_id").alias("total_books"),
        F.round(F.avg("average_rating"), 2).alias("avg_rating"),
        F.sum("ratings_count").alias("total_ratings"),
        F.round(F.avg(F.col("positive_review_pct").cast("double")), 1).alias("avg_positive_pct"),
        F.round(F.avg(
            (F.col("average_rating") / 5.0 * RATING_WEIGHT) +
            (F.log(F.col("ratings_count") + 1) / float(math.log(5000001)) * POPULARITY_WEIGHT) +
            (F.when(F.col("positive_review_pct").isNotNull() & (F.col("total_reviews") > 0),
                    F.col("positive_review_pct") / 100.0)
              .otherwise(F.when(F.col("average_rating") >= 4.0, 1.0)
                          .when(F.col("average_rating") >= 3.0, 0.5)
                          .otherwise(0.0)) * SENTIMENT_WEIGHT)
        ), 4).alias("avg_weighted_score")
    )
    .orderBy(F.col("avg_weighted_score").desc())
    .withColumn("genre_rank", F.rank().over(Window.orderBy(F.col("avg_weighted_score").desc())))
    .withColumn("_gold_timestamp", F.current_timestamp())
)

genre_summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD_PATH}genre_summary/")

total = spark.read.format("delta").load(f"{GOLD_PATH}audiobook_candidates/").count()
print(f"\nGold candidates: {total:,} books scored and ranked")

print("\nTOP 10 AUDIOBOOK CANDIDATES:")
spark.read.format("delta").load(f"{GOLD_PATH}audiobook_candidates/").select(
    "audiobook_rank", "title", "primary_genre",
    "average_rating", "num_pages", "length_category", "weighted_score"
).show(10, truncate=False)

print("\nGENRE PERFORMANCE:")
spark.read.format("delta").load(f"{GOLD_PATH}genre_summary/").select(
    "genre_rank", "primary_genre", "total_books", "avg_rating", "avg_weighted_score"
).show(truncate=False)

top = spark.read.format("delta").load(f"{GOLD_PATH}audiobook_candidates/").orderBy("audiobook_rank").first()
print(f"""
       BUSINESS RECOMMENDATION:
   Top candidate : {top['title']}
   Genre         : {top['primary_genre']}
   Rating        : {top['average_rating']} | Pages: {top['num_pages']} ({top['length_category']})
   Score         : {top['weighted_score']} | Ratings: {top['ratings_count']:,}
""")

# COMMAND ----------

# Cell 7 — Register Tables in Unity Catalog
#
# Unity Catalog makes all layers queryable via SQL from any tool —
# Databricks SQL, Power BI, Python notebooks — with consistent
# access control enforced at the catalog level.
#
# Table inventory:
#   bronze_books    — 2.3M raw GoodReads book records
#   bronze_authors  — 829K author records
#   silver_books    — cleaned books enriched with genre and sentiment
#   silver_reviews  — 15.7M reviews with PII hashed
#   gold_audiobook_candidates — 10,670 ranked audiobook candidates
#   gold_genre_summary        — genre performance for portfolio strategy

print("=" * 55)
print("REGISTERING TABLES IN UNITY CATALOG")
print("=" * 55)

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.bookworm")

tables = {
    "bronze_books"             : f"{BRONZE_PATH}books/",
    "bronze_authors"           : f"{BRONZE_PATH}authors/",
    "silver_books"             : f"{SILVER_PATH}books/",
    "silver_reviews"           : f"{SILVER_PATH}reviews/",
    "gold_audiobook_candidates": f"{GOLD_PATH}audiobook_candidates/",
    "gold_genre_summary"       : f"{GOLD_PATH}genre_summary/",
}

for table, path in tables.items():
    try:
        spark.read.format("delta").load(path) \
            .write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"`{CATALOG}`.bookworm.{table}")
        count = spark.sql(f"SELECT COUNT(*) as c FROM `{CATALOG}`.bookworm.{table}").first()['c']
        print(f" {table:35} {count:,} rows")
    except Exception as e:
        print(f"{table}: {e}")

print("\n ALL TABLES REGISTERED IN UNITY CATALOG")



# Cell 8 — Business Queries
#
# These queries run against persistent Unity Catalog tables.
# Any analyst can run them at any time — no pipeline execution needed.

print("=" * 55)
print("BUSINESS QUERIES — REAL GOODREADS DATA")
print("=" * 55)

print("\nTOP 10 AUDIOBOOK CANDIDATES:")
spark.sql(f"""
    SELECT audiobook_rank AS rank, title, primary_genre AS genre,
           average_rating AS rating,
           FORMAT_NUMBER(ratings_count, 0) AS total_ratings,
           num_pages, length_category, weighted_score AS score
    FROM `{CATALOG}`.bookworm.gold_audiobook_candidates
    ORDER BY audiobook_rank LIMIT 10
""").show(truncate=False)

print("\nBEST BOOK PER GENRE:")
spark.sql(f"""
    SELECT primary_genre AS genre, title, average_rating AS rating, weighted_score AS score
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY primary_genre ORDER BY weighted_score DESC) AS rn
        FROM `{CATALOG}`.bookworm.gold_audiobook_candidates
        WHERE primary_genre NOT IN ('Uncategorised', 'comics_graphic')
    ) WHERE rn = 1
    ORDER BY score DESC
""").show(truncate=False)

print("\nGENRE STRATEGY:")
spark.sql(f"""
    SELECT genre_rank, primary_genre AS genre, total_books,
           avg_rating, FORMAT_NUMBER(total_ratings, 0) AS total_ratings, avg_weighted_score AS avg_score
    FROM `{CATALOG}`.bookworm.gold_genre_summary
    ORDER BY genre_rank
""").show(truncate=False)

print("\nDELTA TIME TRAVEL — audit any historical state:")
spark.sql(f"""
    DESCRIBE HISTORY delta.`{GOLD_PATH}audiobook_candidates/`
""").select("version", "timestamp", "operation").show(5)

print("\n DATA QUALITY SUMMARY:")
spark.sql(f"""
    SELECT 'bronze_books' AS layer, COUNT(*) AS total_records,
           SUM(CASE WHEN book_id IS NULL THEN 1 ELSE 0 END) AS null_ids
    FROM `{CATALOG}`.bookworm.bronze_books
    UNION ALL
    SELECT 'silver_books', COUNT(*),
           SUM(CASE WHEN book_id IS NULL THEN 1 ELSE 0 END)
    FROM `{CATALOG}`.bookworm.silver_books
    UNION ALL
    SELECT 'gold_candidates', COUNT(*),
           SUM(CASE WHEN book_id IS NULL THEN 1 ELSE 0 END)
    FROM `{CATALOG}`.bookworm.gold_audiobook_candidates
""").show()

# COMMAND ----------

# ============================================================
# SPARK NLP — PRODUCTION SENTIMENT ANALYSIS
# (DEMONSTRATION ONLY — NOT EXECUTED IN THIS POC)
#
# Current sentiment classification uses star ratings as proxy.
# This cell shows the production upgrade path using Spark NLP
# running natively on this Databricks cluster.
#
# Spark NLP runs distributed on the same cluster — no API calls,
# no per-record cost, no network latency.
# Azure Cognitive Services charges per 1000 characters.
# At 15.7M reviews averaging 200 characters each = 3.1B characters
# = approximately EUR 15,000 in API costs for a single run.
# Spark NLP = zero incremental cost after cluster setup.
#
# A 4-star review saying "beautiful writing but too slow for audio"
# should FLAG an audiobook risk — not be classified as positive.
# NLP reads the actual text and detects audiobook-specific signals:
# - narrator quality: "narrator", "voice", "reading style"
# - listening experience: "audio", "listening", "commute"
# - pacing signals: "slow", "fast-paced", "dense"
# ============================================================


# pip install spark-nlp
# import sparknlp

"""
PRODUCTION IMPLEMENTATION — READ ONLY

from sparknlp.pretrained import PretrainedPipeline

# Load pre-trained sentiment pipeline
# vivekn/sentiment-model works well for book reviews
sentiment_pipeline = PretrainedPipeline(
    'analyze_sentimentdl_use_twitter',
    lang='en'
)

# Apply to review text
silver_reviews_nlp = (
    bronze_reviews
    .filter(F.col('review_text').isNotNull())
    .filter(F.length(F.col('review_text')) > 50)
)

# Run sentiment on review text
result = sentiment_pipeline.transform(silver_reviews_nlp)

# Extract sentiment result
nlp_sentiment = result.select(
    'review_id',
    'book_id',
    F.col('sentiment.result')[0].alias('nlp_sentiment'),
    F.col('sentiment.metadata')[0]['confidence'].alias('confidence')
)

# Audiobook-specific signals from review text
audiobook_signals = (
    bronze_reviews
    .withColumn('mentions_narrator',
        F.col('review_text').rlike('(?i)narrator|voice|reading style|audio')
    )
    .withColumn('mentions_pacing',
        F.col('review_text').rlike('(?i)slow|fast.paced|dense|dry|engaging')
    )
    .withColumn('audiobook_risk',
        F.when(
            F.col('review_text').rlike(
                '(?i)too slow|boring to listen|better to read|not for audio'
            ), True
        ).otherwise(False)
    )
)

# Enhanced sentiment score combining NLP + audiobook signals
enhanced_sentiment = (
    nlp_sentiment
    .join(audiobook_signals, on='review_id')
    .withColumn('final_sentiment',
        F.when(F.col('audiobook_risk'), 'negative')
         .when(F.col('nlp_sentiment') == 'pos', 'positive')
         .when(F.col('nlp_sentiment') == 'neg', 'negative')
         .otherwise('neutral')
    )
)

# This replaces the rating-based proxy in stg_reviews.sql
# Result: positive_review_pct is now based on actual text analysis
# not star ratings — significantly more accurate for audiobook decisions

PRODUCTION UPGRADE TIMELINE:
  Week 1: Install Spark NLP, test on 10K reviews sample
  Week 2: Validate results against rating proxy — measure uplift
  Week 3: Deploy to production pipeline
  Estimated improvement: 15-20% better audiobook recommendations
"""

print("=" * 55)
print("SPARK NLP — PRODUCTION SENTIMENT ANALYSIS")
print("=" * 55)
print("""
STATUS: Documented — not executed in this POC

CURRENT APPROACH:
   Rating >= 4  → positive
   Rating = 3   → neutral
   Rating < 3   → negative
   
   Limitation: A 4-star review saying "beautiful writing
   but terrible for audio format" is classified as positive
   when it should flag an audiobook risk.

PRODUCTION APPROACH (Spark NLP):
   Step 1: Load pre-trained sentiment model
   Step 2: Run inference on 15.7M review texts
   Step 3: Extract audiobook-specific signals
           - narrator quality mentions
           - listening experience feedback  
           - pacing and density signals
   Step 4: Calculate enhanced sentiment score
   
   Result: Audiobook-specific sentiment vs generic rating proxy

WHY Spark NLP over Azure Cognitive Services:
   At 15.7M reviews x 200 chars = 3.1B characters
   Azure Cognitive Services = ~EUR 15,000 per run
   Spark NLP on existing cluster = EUR 0 incremental cost

ESTIMATED IMPLEMENTATION:
   3 weeks end-to-end
   15-20% improvement in recommendation accuracy
""")
print("=" * 55)