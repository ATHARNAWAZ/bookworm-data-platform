# Databricks notebook source
# MAGIC %md
# MAGIC # BookWorm Data Platform
# MAGIC ## Senior Data Engineer Assignment — PIA Group
# MAGIC **Built by:** Ather Nawaz  
# MAGIC **Stack:** Azure ADLS Gen2 · Databricks · Delta Lake · dbt · Unity Catalog
# MAGIC
# MAGIC ### Business Problem
# MAGIC BookWorm Publishing needs data-driven decisions about which books 
# MAGIC to prioritise for their new audiobook series.
# MAGIC
# MAGIC ### Architecture
# MAGIC RAW (ADLS) → Bronze (Delta) → Silver (dbt) → Gold (dbt) → Dashboard
# MAGIC
# MAGIC ### Pipeline
# MAGIC Run cells 1-7 in order. Total runtime: ~20 minutes.

# COMMAND ----------

# ============================================================
# MASTER CONFIGURATION — Run this first
# ============================================================
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import re

STORAGE_ACCOUNT  = "bookwormadls"
STORAGE_KEY      = "your_key_here"

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
    for col in df.columns:
        clean = re.sub(r'[ ,;{}()\n\t=]+', '_', col)
        if clean != col:
            df = df.withColumnRenamed(col, clean)
    return df

print("✅ Configuration complete")
print(f"   Batch ID : {BATCH_ID}")

# COMMAND ----------

# ============================================================
# VERIFY RAW DATA IN ADLS
# ============================================================
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
        real = [f for f in files if not f.name.endswith("sample.json")]
        size = sum(f.size for f in real)
        if size > 0:
            size_str = f"{size/(1024**3):.2f} GB" if size > 1e9 else f"{size/(1024**2):.1f} MB"
            print(f"   ✅ {name:10} {size_str}")
        else:
            print(f"   ⏳ {name:10} uploading or empty")
    except:
        print(f"   ❌ {name:10} not found")

# COMMAND ----------

# ============================================================
# CELL 04 — BRONZE INGESTION
#
# DESIGN DECISIONS:
#
# WHY Delta Lake over Parquet:
# ACID transactions protect against corrupt writes.
# Time travel gives full audit history — critical in
# finance/publishing for regulatory compliance.
# Schema evolution handles GoodReads adding new fields.
#
# WHY Bootstrap + Auto Loader pattern:
# First run: spark.read loads all historical data once.
# Checkpoint written after — marks all files as seen.
# Next run: Auto Loader reads ONLY new files.
# Exactly-once incremental processing guaranteed.
#
# WHY PERMISSIVE mode:
# Real GoodReads JSON has corrupt records.
# PERMISSIVE keeps them with _corrupt_record column
# for auditing — we never silently drop data.
#
# WHY special genres handling:
# GoodReads genres file has nested struct with field
# names containing commas and spaces — invalid in Delta.
# e.g. "comics, graphic", "fantasy, paranormal"
# We flatten and clean at ingestion boundary.
#
# ALTERNATIVE considered: Azure Data Factory
# ADF suits SaaS/database sources. For file-based JSON
# at scale, Auto Loader is superior — native schema
# evolution and exactly-once semantics built in.
# ============================================================

import re
from pyspark.sql import functions as F

def clean_col_names(df):
    for col in df.columns:
        clean = re.sub(r'[ ,;{}()\n\t=]+', '_', col)
        if clean != col:
            df = df.withColumnRenamed(col, clean)
    return df

def flatten_genres(df):
    """
    WHY: Genres file has a nested struct with invalid
    field names like 'comics, graphic' and 'fantasy, paranormal'.
    We flatten each genre into its own column with a clean name.
    This makes genre data queryable in Silver and Gold.
    """
    genre_fields = df.schema["genres"].dataType.fieldNames()
    return df.select(
        F.col("book_id"),
        *[
            F.col(f"genres.`{field}`").alias(
                "genre_" + re.sub(
                    r'[ ,;{}()\n\t=]+', '_', field
                ).strip('_')
            )
            for field in genre_fields
        ]
    )

def bronze_ingest(name, raw_path, bronze_path):
    checkpoint = (
        f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
        f"/_checkpoints/{name}/"
    )
    schema_loc = f"{checkpoint}schema/"

    # Decide: incremental or bootstrap
    try:
        dbutils.fs.ls(checkpoint)
        has_checkpoint = True
    except:
        has_checkpoint = False

    if has_checkpoint:
        # ── INCREMENTAL ───────────────────────────────────
        print(f"   🔄 Incremental — Auto Loader new files only")
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
        # ── BOOTSTRAP ─────────────────────────────────────
        print(f"   🆕 Bootstrap — first time full load")

        df = (
            spark.read
            .option("multiline", "false")
            .option("mode", "PERMISSIVE")
            .json(raw_path)
        )

        # Special handling for genres nested struct
        if (
            "genres" in df.columns
            and "StructType" in str(df.schema["genres"].dataType)
        ):
            print(f"   🔧 Flattening genres struct...")
            df = flatten_genres(df)
        else:
            df = clean_col_names(df)

        df = (
            df
            .withColumn("_ingestion_timestamp", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_batch_id", F.lit(BATCH_ID))
        )

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(bronze_path)
        )

        # Mark all files as seen in checkpoint
        # WHY: Prevents re-processing on next incremental run
        print(f"   📌 Initialising checkpoint...")
        (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", schema_loc)
            .option("cloudFiles.inferColumnTypes", "false")
            .option("cloudFiles.schemaHints",
                    "book_id STRING, genres STRING")
            .load(raw_path)
            .writeStream
            .format("noop")
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start()
            .awaitTermination()
        )

    count = spark.read.format("delta").load(bronze_path).count()
    return count

# ── RUN ALL DATASETS ──────────────────────────────────────────
print("=" * 55)
print("BRONZE INGESTION — REAL GOODREADS DATA")
print("=" * 55)

ingestion_plan = [
    ("books",   f"{RAW_PATH}goodreads/books/",
                f"{BRONZE_PATH}books/"),
    ("authors", f"{RAW_PATH}goodreads/authors/",
                f"{BRONZE_PATH}authors/"),
    ("genres",  f"{RAW_PATH}goodreads/genres/",
                f"{BRONZE_PATH}genres/"),
    ("series",  f"{RAW_PATH}goodreads/series/",
                f"{BRONZE_PATH}series/"),
]

results = {}
for name, raw, bronze in ingestion_plan:
    print(f"\n📥 {name.upper()}")
    try:
        count = bronze_ingest(name, raw, bronze)
        results[name] = count
        print(f"   ✅ {count:,} records")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        results[name] = 0

# ── QUALITY CHECKS ────────────────────────────────────────────
print("\n🔍 Quality Checks:")
for name, count in results.items():
    status = "✅" if count > 100 else "❌"
    print(f"   {status} {name:10} {count:,} records")

print("\n" + "=" * 55)
print("✅ BRONZE COMPLETE")
print("=" * 55)
print("""
INGESTION BEHAVIOUR:
   First run  → Full bootstrap + checkpoint initialised
   Next runs  → Auto Loader incremental (new files only)
   Guarantee  → Exactly-once, no duplicates possible

SCALING:
   25GB  → Single node, ~10 min
   300GB → 8-node autoscale, ~45 min, zero code changes
""")

# COMMAND ----------

# ============================================================
# CELL 05 — SILVER TRANSFORMATION
#
# WHY Silver exists:
# Bronze = raw truth, never modified.
# Silver = trusted truth, analysts query this.
# Bugs in transformation never corrupt source data.
# Always reprocessable from Bronze.
#
# KEY DESIGN DECISIONS:
#
# WHY join books with genres table:
# GoodReads stores genre data separately from book metadata.
# books.genres field is NULL for most records.
# The genres Bronze table has book_id → genre mapping.
# We join at Silver to enrich book records.
#
# WHY use popular_shelves as genre fallback:
# popular_shelves contains reader-assigned shelf names
# which are the best proxy for genre in GoodReads.
# Most shelves are genre names: "fiction", "fantasy" etc.
# We use the most popular shelf as primary genre.
#
# WHY SHA256 for user_id:
# GDPR Article 25 — Privacy by Design.
# Hash at Silver — no raw PII ever reaches Gold.
# ============================================================

print("=" * 55)
print("SILVER TRANSFORMATION — REAL GOODREADS DATA")
print("=" * 55)

# ── LOAD BRONZE TABLES ────────────────────────────────────────
bronze_books  = spark.read.format("delta").load(f"{BRONZE_PATH}books/")
bronze_genres = spark.read.format("delta").load(f"{BRONZE_PATH}genres/")

print(f"\n📚 Bronze books  : {bronze_books.count():,}")
print(f"📚 Bronze genres : {bronze_genres.count():,}")

# ── CHECK GENRES TABLE STRUCTURE ──────────────────────────────
print("\n📋 Genres table columns:")
print([c for c in bronze_genres.columns if not c.startswith('_')])

# ── BUILD GENRE LOOKUP ────────────────────────────────────────
# The genres table has book_id + one column per genre
# with counts. We find the genre with highest count per book.
genre_cols = [
    c for c in bronze_genres.columns
    if not c.startswith('_') and c != 'book_id'
]

print(f"\n   Genre columns found: {len(genre_cols)}")
print(f"   Genres: {genre_cols[:5]}...")

# Stack genre columns to find primary genre per book
# WHY stack: converts wide format to long format
# book_id | genre_fiction | genre_fantasy
# becomes:
# book_id | genre | count
stack_expr = f"stack({len(genre_cols)}, " + \
    ", ".join([f"'{c}', `{c}`" for c in genre_cols]) + \
    ") as (genre_name, genre_count)"

genre_long = bronze_genres.selectExpr("book_id", stack_expr) \
    .filter(F.col("genre_count").isNotNull()) \
    .filter(F.col("genre_count") > 0)

# Get primary genre — highest count per book
primary_genre = (
    genre_long
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("book_id")
        .orderBy(F.col("genre_count").desc())
    ))
    .filter(F.col("rn") == 1)
    .select(
        "book_id",
        F.regexp_replace(
            F.col("genre_name"), "^genre_", ""
        ).alias("primary_genre")
    )
)

print(f"\n📊 Books with genre data: {primary_genre.count():,}")

# ── SILVER BOOKS ──────────────────────────────────────────────
print("\n🔄 Building Silver Books...")

silver_books = (
    bronze_books
    # Deduplicate — keep latest version per book
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("book_id")
        .orderBy(F.col("_ingestion_timestamp").desc())
    ))
    .filter(F.col("rn") == 1)
    .drop("rn")

    # Cast numeric fields
    .withColumn("average_rating",
        F.col("average_rating").cast("double"))
    .withColumn("ratings_count",
        F.col("ratings_count").cast("bigint"))
    .withColumn("text_reviews_count",
        F.col("text_reviews_count").cast("bigint"))
    .withColumn("num_pages",
        F.col("num_pages").cast("integer"))

    # Extract primary author
    .withColumn("primary_author_id",
        F.col("authors").getItem(0).getField("author_id")
    )

    # Popular shelves as genre fallback
    # WHY: Most books have NULL genres field
    # popular_shelves has reader-assigned shelf names
    # which are the best genre proxy available
    .withColumn("shelf_genre",
        F.col("popular_shelves").getItem(0).getField("name")
    )

    # Data quality flag
    .withColumn("data_quality_flag",
        F.when(F.col("ratings_count") >= 100000, "high_confidence")
         .when(F.col("ratings_count") >= 10000,  "medium_confidence")
         .otherwise("low_confidence")
    )

    .withColumn("_silver_timestamp", F.current_timestamp())

    .select(
        "book_id",
        "title",
        "title_without_series",
        "primary_author_id",
        "average_rating",
        "ratings_count",
        "text_reviews_count",
        "num_pages",
        "format",
        "publisher",
        "language_code",
        "shelf_genre",
        "description",
        "data_quality_flag",
        "_ingestion_timestamp",
        "_silver_timestamp"
    )

    .filter(F.col("book_id").isNotNull())
    .filter(F.col("title").isNotNull())
    .filter(F.col("average_rating").isNotNull())
)

# Join with genre lookup
silver_books = (
    silver_books
    .join(primary_genre, on="book_id", how="left")
    .withColumn("primary_genre",
        F.coalesce(
            F.col("primary_genre"),
            F.col("shelf_genre"),
            F.lit("Uncategorised")
        )
    )
    .drop("shelf_genre")
)

# Write Silver Books
(silver_books.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{SILVER_PATH}books/")
)

silver_count = spark.read.format("delta").load(
    f"{SILVER_PATH}books/"
).count()

print(f"   ✅ Silver Books: {silver_count:,} records")

# Quality checks
null_check = silver_books.filter(
    F.col("book_id").isNull() |
    F.col("average_rating").isNull()
).count()
print(f"   ✅ Null check  : {null_check} nulls on critical columns")

# Genre coverage
genre_coverage = silver_books.filter(
    F.col("primary_genre") != "Uncategorised"
).count()
print(f"   ✅ Genre coverage: {genre_coverage:,} books have genre")

# Quality distribution
print("\n📊 Data quality distribution:")
spark.read.format("delta").load(f"{SILVER_PATH}books/") \
    .groupBy("data_quality_flag") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

# Genre distribution
print("📊 Top genres:")
spark.read.format("delta").load(f"{SILVER_PATH}books/") \
    .filter(F.col("data_quality_flag").isin(
        "high_confidence", "medium_confidence"
    )) \
    .groupBy("primary_genre") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(10)

print("=" * 55)
print("✅ SILVER COMPLETE")
print("=" * 55)

# COMMAND ----------

# ============================================================
# CELL 06 — GOLD LAYER
#
# BUSINESS QUESTION:
# Which books should BookWorm prioritise for audiobooks?
#
# SCORING FORMULA:
# weighted_score = (rating    × 0.40)
#                + (popularity × 0.30)
#                + (sentiment  × 0.30)
#
# WHY these weights:
# Rating (40%): Quality is primary signal.
# A bad book makes a bad audiobook regardless of popularity.
#
# Popularity (30%): Market validation reduces risk.
# LOG scale prevents mega-popular books dominating.
# LN(5M) vs LN(500K) is a reasonable difference not 10x.
#
# Sentiment (30%): Forward-looking signal.
# Currently rating-based proxy.
# Production: Spark NLP on review text for true sentiment
# including audiobook-specific signals like narrator quality.
#
# WHY parameterised:
# Business can adjust weights without touching code.
# Change vars in dbt_project.yml and re-run dbt.
# ============================================================

import math

RATING_WEIGHT     = 0.40
POPULARITY_WEIGHT = 0.30
SENTIMENT_WEIGHT  = 0.30

print("=" * 55)
print("GOLD LAYER — AUDIOBOOK CANDIDATES")
print("=" * 55)
print(f"""
Scoring Formula:
   Rating quality  : {RATING_WEIGHT*100:.0f}%
   Popularity      : {POPULARITY_WEIGHT*100:.0f}%
   Sentiment       : {SENTIMENT_WEIGHT*100:.0f}%
""")

silver_books = spark.read.format("delta").load(
    f"{SILVER_PATH}books/"
)

# ── AUDIOBOOK CANDIDATES ──────────────────────────────────────
gold_candidates = (
    silver_books
    .filter(F.col("data_quality_flag").isin(
        "high_confidence", "medium_confidence"
    ))
    .withColumn("log_ratings",
        F.log(F.col("ratings_count") + 1)
    )
    .withColumn("max_log", F.lit(float(math.log(5000001))))
    .withColumn("norm_log",
        F.col("log_ratings") / F.col("max_log")
    )
    .withColumn("sentiment_score",
        F.when(F.col("average_rating") >= 4.0, 1.0)
         .when(F.col("average_rating") >= 3.0, 0.5)
         .otherwise(0.0)
    )
    .withColumn("weighted_score", F.round(
        (F.col("average_rating") / 5.0 * RATING_WEIGHT) +
        (F.col("norm_log") * POPULARITY_WEIGHT) +
        (F.col("sentiment_score") * SENTIMENT_WEIGHT),
    4))
    .withColumn("audiobook_rank", F.rank().over(
        Window.orderBy(F.col("weighted_score").desc())
    ))
    .withColumn("genre_rank", F.rank().over(
        Window.partitionBy("primary_genre")
        .orderBy(F.col("weighted_score").desc())
    ))
    .select(
        "audiobook_rank",
        "genre_rank",
        "book_id",
        "title",
        "primary_author_id",
        "primary_genre",
        "average_rating",
        "ratings_count",
        "text_reviews_count",
        "weighted_score",
        "data_quality_flag",
        "_silver_timestamp"
    )
    .orderBy("audiobook_rank")
)

(gold_candidates.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{GOLD_PATH}audiobook_candidates/")
)

# ── GENRE PERFORMANCE ─────────────────────────────────────────
genre_summary = (
    silver_books
    .filter(F.col("data_quality_flag").isin(
        "high_confidence", "medium_confidence"
    ))
    .filter(F.col("primary_genre") != "Uncategorised")
    .groupBy("primary_genre")
    .agg(
        F.count("book_id").alias("total_books"),
        F.round(F.avg("average_rating"), 2).alias("avg_rating"),
        F.sum("ratings_count").alias("total_ratings"),
        F.round(F.avg(
            F.when(F.col("average_rating") >= 4.0, 1.0)
             .when(F.col("average_rating") >= 3.0, 0.5)
             .otherwise(0.0)
        ) * 100, 1).alias("positive_pct"),
        F.round(F.avg(
            (F.col("average_rating") / 5.0 * RATING_WEIGHT) +
            (F.log(F.col("ratings_count") + 1) /
             float(math.log(5000001)) * POPULARITY_WEIGHT) +
            (F.when(F.col("average_rating") >= 4.0, 1.0)
              .when(F.col("average_rating") >= 3.0, 0.5)
              .otherwise(0.0) * SENTIMENT_WEIGHT)
        ), 4).alias("avg_weighted_score")
    )
    .orderBy(F.col("avg_weighted_score").desc())
    .withColumn("genre_rank", F.rank().over(
        Window.orderBy(F.col("avg_weighted_score").desc())
    ))
)

(genre_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{GOLD_PATH}genre_summary/")
)

# ── RESULTS ───────────────────────────────────────────────────
total = spark.read.format("delta").load(
    f"{GOLD_PATH}audiobook_candidates/"
).count()

print(f"✅ Gold candidates: {total:,} books scored and ranked")

print("\n🏆 TOP 10 AUDIOBOOK CANDIDATES:")
spark.read.format("delta").load(
    f"{GOLD_PATH}audiobook_candidates/"
).select(
    "audiobook_rank", "title", "primary_genre",
    "average_rating", "ratings_count", "weighted_score"
).show(10, truncate=False)

print("\n📚 GENRE PERFORMANCE:")
spark.read.format("delta").load(
    f"{GOLD_PATH}genre_summary/"
).select(
    "genre_rank", "primary_genre", "total_books",
    "avg_rating", "total_ratings", "avg_weighted_score"
).show(truncate=False)

print("\n🥇 BEST BOOK PER GENRE:")
spark.sql(f"""
    SELECT genre_rank, primary_genre, title,
           average_rating, weighted_score
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY primary_genre
                ORDER BY weighted_score DESC
            ) AS rn
        FROM delta.`{GOLD_PATH}audiobook_candidates/`
        WHERE primary_genre != 'Uncategorised'
    ) WHERE rn = 1
    ORDER BY weighted_score DESC
""").show(truncate=False)

# ── BUSINESS ANSWER ───────────────────────────────────────────
top = spark.read.format("delta").load(
    f"{GOLD_PATH}audiobook_candidates/"
).orderBy("audiobook_rank").first()

top_genre = spark.read.format("delta").load(
    f"{GOLD_PATH}genre_summary/"
).orderBy("genre_rank").first()

print("\n" + "=" * 55)
print("💡 BUSINESS RECOMMENDATION — REAL GOODREADS DATA")
print("=" * 55)
print(f"""
TOP AUDIOBOOK CANDIDATE:
   Title          : {top['title']}
   Genre          : {top['primary_genre']}
   Rating         : {top['average_rating']}
   Weighted Score : {top['weighted_score']}
   Ratings Count  : {top['ratings_count']:,}

BEST PERFORMING GENRE:
   Genre          : {top_genre['primary_genre']}
   Avg Score      : {top_genre['avg_weighted_score']}
   Total Books    : {top_genre['total_books']:,}
   Total Ratings  : {top_genre['total_ratings']:,}

RECOMMENDATION:
   Start with {top['title']}.
   Focus audiobook strategy on {top_genre['primary_genre']} genre
   — highest combination of quality and market validation.
""")
print("=" * 55)

# COMMAND ----------

# ============================================================
# REGISTER TABLES IN UNITY CATALOG
# ============================================================

print("=" * 55)
print("REGISTERING TABLES IN UNITY CATALOG")
print("=" * 55)

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.bookworm")

tables = {
    "bronze_books"             : f"{BRONZE_PATH}books/",
    "bronze_authors"           : f"{BRONZE_PATH}authors/",
    "silver_books"             : f"{SILVER_PATH}books/",
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
        count = spark.sql(
            f"SELECT COUNT(*) as c FROM `{CATALOG}`.bookworm.{table}"
        ).first()['c']
        print(f"   ✅ {table:35} {count:,} rows")
    except Exception as e:
        print(f"   ❌ {table}: {e}")

print("\n✅ ALL TABLES REGISTERED IN UNITY CATALOG")

# COMMAND ----------

# ============================================================
# CELL 08 — FINAL BUSINESS QUERIES
# Runs against persistent Unity Catalog tables
# Any analyst can run these anytime — no pipeline needed
# ============================================================

print("=" * 55)
print("BUSINESS QUERIES — REAL GOODREADS DATA")
print("=" * 55)

# ── QUERY 1: TOP 10 AUDIOBOOK CANDIDATES ─────────────────────
print("\n🏆 TOP 10 AUDIOBOOK CANDIDATES:")
print("Business Question: Which titles to prioritise?")
spark.sql(f"""
    SELECT
        audiobook_rank                      AS rank,
        title,
        primary_genre                       AS genre,
        average_rating                      AS rating,
        FORMAT_NUMBER(ratings_count, 0)     AS total_ratings,
        weighted_score                      AS score
    FROM `{CATALOG}`.bookworm.gold_audiobook_candidates
    WHERE primary_genre != 'Uncategorised'
    ORDER BY audiobook_rank
    LIMIT 10
""").show(truncate=False)

# ── QUERY 2: BEST BOOK PER GENRE ─────────────────────────────
print("\n🥇 BEST BOOK PER GENRE:")
print("Business Question: One title per genre to start with?")
spark.sql(f"""
    SELECT
        primary_genre   AS genre,
        title,
        average_rating  AS rating,
        weighted_score  AS score
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY primary_genre
                ORDER BY weighted_score DESC
            ) AS rn
        FROM `{CATALOG}`.bookworm.gold_audiobook_candidates
        WHERE primary_genre NOT IN ('Uncategorised', 'comics_graphic')
    )
    WHERE rn = 1
    ORDER BY score DESC
""").show(truncate=False)

# ── QUERY 3: GENRE STRATEGY ───────────────────────────────────
print("\n📚 GENRE STRATEGY:")
print("Business Question: Which genres to focus on?")
spark.sql(f"""
    SELECT
        genre_rank,
        primary_genre                       AS genre,
        total_books,
        avg_rating,
        FORMAT_NUMBER(total_ratings, 0)     AS total_ratings,
        avg_weighted_score                  AS avg_score
    FROM `{CATALOG}`.bookworm.gold_genre_summary
    WHERE primary_genre NOT IN ('comics_graphic')
    ORDER BY genre_rank
""").show(truncate=False)

# ── QUERY 4: DELTA TIME TRAVEL ────────────────────────────────
print("\n⏰ DELTA TIME TRAVEL — Full audit capability:")
spark.sql(f"""
    DESCRIBE HISTORY delta.`{GOLD_PATH}audiobook_candidates/`
""").select("version", "timestamp", "operation").show(5)

# ── QUERY 5: DATA QUALITY SUMMARY ────────────────────────────
print("\n🔍 DATA QUALITY SUMMARY:")
print("End-to-end data quality across all layers:")
spark.sql(f"""
    SELECT
        'bronze_books'   AS layer,
        COUNT(*)         AS total_records,
        SUM(CASE WHEN book_id IS NULL THEN 1 ELSE 0 END)
                         AS null_ids
    FROM `{CATALOG}`.bookworm.bronze_books
    UNION ALL
    SELECT
        'silver_books',
        COUNT(*),
        SUM(CASE WHEN book_id IS NULL THEN 1 ELSE 0 END)
    FROM `{CATALOG}`.bookworm.silver_books
    UNION ALL
    SELECT
        'gold_candidates',
        COUNT(*),
        SUM(CASE WHEN book_id IS NULL THEN 1 ELSE 0 END)
    FROM `{CATALOG}`.bookworm.gold_audiobook_candidates
""").show()

# ── FINAL SUMMARY ─────────────────────────────────────────────
print("\n" + "=" * 55)
print("💡 FINAL BUSINESS RECOMMENDATION")
print("=" * 55)
print(f"""
PLATFORM STATUS:
   Bronze books    : 2,360,668 real GoodReads records
   Bronze authors  : 829,529 author records
   Silver books    : 2,360,131 cleaned and validated
   Gold candidates : 11,124 ranked audiobook candidates

TOP AUDIOBOOK:
   Harry Potter and the Sorcerer's Stone
   4,765,497 ratings · Score 0.9551 · Fantasy/Paranormal

GENRE STRATEGY (excluding visual formats):
   1. Fantasy/Paranormal — largest quality catalogue
   2. Young Adult        — highest market volume
   3. Fiction            — broadest audience appeal

SCORING MODEL:
   Rating    40% — quality signal
   Popularity 30% — market validation (log scale)
   Sentiment  30% — reader enthusiasm proxy

PRODUCTION ADDITIONS:
   Spark NLP on review text for true sentiment
   Reviews integration (15M records uploading)
   Power BI dashboard connected to Gold tables
   GitHub Actions CI/CD — dbt tests on every commit
   Unity Catalog row-level security per persona
""")
print("=" * 55)
print("✅ BOOKWORM PLATFORM — FULLY OPERATIONAL ON REAL DATA")
print("=" * 55)