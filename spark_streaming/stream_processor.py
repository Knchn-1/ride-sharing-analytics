"""
stream_processor.py
====================
Real-Time Ride-Sharing Analytics — PySpark Structured Streaming Pipeline

Streams:
    1. ride_events       — raw ride requests
    2. driver_events     — raw driver location updates
    3. surge_pricing     — 2-minute tumbling window demand-based pricing
    4. driver_zones      — grid-based zone demand ranking

Storage:
    - PostgreSQL         — fact/dimension tables (star schema)
    - Parquet            — partitioned data lake (year/month/day)
    - Dead Letter Queue  — invalid/failed records → Kafka topic

Author: Kanchan
"""

import os
import time
import logging
from datetime import datetime
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, round as spark_round,
    when, concat_ws, floor, lit, desc, year, month, dayofmonth,
    hour, to_timestamp, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType,
    DoubleType, StringType, TimestampType
)

# ══════════════════════════════════════════════════════════════════════════════
# LOGGING SETUP
# ══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),                          # console
        logging.FileHandler("logs/stream_processor.log") # file
    ]
)
logger = logging.getLogger("RideStreamingAnalytics")

# Create logs directory if not exists
os.makedirs("logs", exist_ok=True)

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG — loaded from .env file, never hardcoded
# ══════════════════════════════════════════════════════════════════════════════

load_dotenv()

PG_HOST     = os.getenv("PG_HOST", "localhost")
PG_PORT     = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "ride_analytics")
PG_USER     = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD")

PARQUET_OUTPUT = "D:/Data_eng_Projects/ride-sharing-analytics/data/output"
PG_JAR         = "file:///D:/Data_eng_Projects/ride-sharing-analytics/postgresql-42.6.0.jar"
KAFKA_SERVERS  = "localhost:9092"
GRID_SIZE      = 0.02   # ~2km grid cells

PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
PG_PROPS = {
    "user":           PG_USER,
    "password":       PG_PASSWORD,
    "driver":         "org.postgresql.Driver",
    "batchsize":      "1000",      # ← connection pooling: batch inserts
    "numPartitions":  "4",         # ← connection pooling: parallel writes
}

# ══════════════════════════════════════════════════════════════════════════════
# SPARK SESSION
# ══════════════════════════════════════════════════════════════════════════════

spark = SparkSession.builder \
    .appName("RideStreamingAnalytics") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.jars", PG_JAR) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.streaming.metricsEnabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
logger.info("✅ Spark session started successfully")

# ══════════════════════════════════════════════════════════════════════════════
# SCHEMAS
# ══════════════════════════════════════════════════════════════════════════════

ride_schema = StructType([
    StructField("ride_id",     IntegerType()),
    StructField("user_id",     IntegerType()),
    StructField("pickup_lat",  DoubleType()),
    StructField("pickup_long", DoubleType()),
    StructField("timestamp",   StringType())
])

driver_schema = StructType([
    StructField("driver_id",   IntegerType()),
    StructField("driver_lat",  DoubleType()),
    StructField("driver_long", DoubleType()),
    StructField("status",      StringType()),
    StructField("timestamp",   StringType())
])

# ══════════════════════════════════════════════════════════════════════════════
# DATA QUALITY VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

def validate_ride(df):
    """
    Validate ride events for data quality.

    Rules:
        - ride_id must not be null
        - pickup_lat must be between 21.0 and 21.3 (Nagpur bounds)
        - pickup_long must be between 78.9 and 79.2 (Nagpur bounds)
        - user_id must not be null

    Returns:
        valid_df:   DataFrame with records passing all checks
        invalid_df: DataFrame with records failing any check (→ DLQ)
    """
    valid_df = df.filter(
        col("ride_id").isNotNull() &
        col("user_id").isNotNull() &
        col("pickup_lat").between(21.0, 21.3) &
        col("pickup_long").between(78.9, 79.2)
    )
    invalid_df = df.subtract(valid_df)
    return valid_df, invalid_df

def validate_driver(df):
    """
    Validate driver location events for data quality.

    Rules:
        - driver_id must not be null
        - status must be one of: available, on_trip, offline
        - coordinates must be within Nagpur bounds

    Returns:
        valid_df:   DataFrame with records passing all checks
        invalid_df: DataFrame with records failing any check (→ DLQ)
    """
    valid_df = df.filter(
        col("driver_id").isNotNull() &
        col("status").isin("available", "on_trip", "offline") &
        col("driver_lat").between(21.0, 21.3) &
        col("driver_long").between(78.9, 79.2)
    )
    invalid_df = df.subtract(valid_df)
    return valid_df, invalid_df

# ══════════════════════════════════════════════════════════════════════════════
# STORAGE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def write_to_postgres(df, epoch_id, table_name):
    """
    Write a Spark streaming micro-batch to a PostgreSQL table.

    Uses batch insert with connection pooling for performance.
    Logs row count and elapsed time per batch.

    Args:
        df:         Spark DataFrame containing the micro-batch data
        epoch_id:   Unique batch identifier (used for idempotency logging)
        table_name: Target PostgreSQL table name
    """
    try:
        start_time = time.time()
        row_count  = df.count()
        if row_count > 0:
            df.write \
              .jdbc(url=PG_URL, table=table_name,
                    mode="append", properties=PG_PROPS)
            elapsed = round((time.time() - start_time) * 1000, 2)
            logger.info(f"✅ [{table_name}] epoch={epoch_id} | "
                        f"rows={row_count} | time={elapsed}ms")
    except Exception as e:
        logger.error(f"❌ [{table_name}] epoch={epoch_id} | "
                     f"PostgreSQL write failed: {e}")

def write_to_parquet(df, epoch_id, path):
    """
    Write a Spark streaming micro-batch to partitioned Parquet files.

    Partitions data by year/month/day for efficient querying.
    Example path: data/output/rides/year=2026/month=3/day=11/

    Args:
        df:       Spark DataFrame containing the micro-batch data
        epoch_id: Unique batch identifier
        path:     Base output path for Parquet files
    """
    try:
        if df.count() > 0:
            df.withColumn("year",  year(current_timestamp())) \
              .withColumn("month", month(current_timestamp())) \
              .withColumn("day",   dayofmonth(current_timestamp())) \
              .write \
              .mode("append") \
              .partitionBy("year", "month", "day") \
              .parquet(path)
            logger.info(f"✅ [parquet] epoch={epoch_id} | written to {path}")
    except Exception as e:
        logger.error(f"❌ [parquet] epoch={epoch_id} | write failed: {e}")

def write_to_dlq(df, epoch_id, source):
    """
    Write invalid records to the Dead Letter Queue (DLQ) PostgreSQL table.

    Records that fail data quality validation are stored here
    for investigation and reprocessing.

    Args:
        df:       Spark DataFrame containing invalid records
        epoch_id: Unique batch identifier
        source:   Source stream name (e.g. 'ride_events', 'driver_events')
    """
    try:
        row_count = df.count()
        if row_count > 0:
            df.withColumn("source",     lit(source)) \
              .withColumn("failed_at",  lit(str(datetime.now()))) \
              .withColumn("epoch_id",   lit(epoch_id)) \
              .write \
              .jdbc(url=PG_URL, table="dead_letter_queue",
                    mode="append", properties=PG_PROPS)
            logger.warning(f"⚠️  [DLQ] {row_count} invalid records from "
                           f"{source} | epoch={epoch_id}")
    except Exception as e:
        logger.error(f"❌ [DLQ] Failed to write to dead letter queue: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# READ FROM KAFKA
# ══════════════════════════════════════════════════════════════════════════════

def read_topic(topic):
    """
    Read a Kafka topic as a Spark structured stream.

    Args:
        topic: Kafka topic name to subscribe to

    Returns:
        Spark streaming DataFrame with 'value' column as STRING
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
        .selectExpr("CAST(value AS STRING) as value")
    )

# ── Parse ride events ──────────────────────────────────────────────────────────

ride_df = (
    read_topic("ride_requests")
    .select(from_json(col("value"), ride_schema).alias("d"))
    .select("d.*")
    .withColumn("event_time", col("timestamp").cast(TimestampType()))
)

# ── Parse driver events ────────────────────────────────────────────────────────

driver_df = (
    read_topic("driver_locations")
    .select(from_json(col("value"), driver_schema).alias("d"))
    .select("d.*")
    .withColumn("event_time", col("timestamp").cast(TimestampType()))
)

# ══════════════════════════════════════════════════════════════════════════════
# STREAM 1 — RIDE EVENTS → Validate → DLQ + PostgreSQL + Parquet
# Star Schema: fact_rides
# ══════════════════════════════════════════════════════════════════════════════

ride_out = ride_df.drop("event_time")

ride_console = (
    ride_out.writeStream
    .format("console")
    .outputMode("append")
    .option("checkpointLocation", "checkpoint/ride_console")
    .trigger(processingTime="5 seconds")
    .queryName("ride_console")
    .start()
)

def process_rides(df, epoch_id):
    """Process ride events — validate, write valid to storage, invalid to DLQ."""
    valid_df, invalid_df = validate_ride(df)
    write_to_postgres(valid_df, epoch_id, "fact_rides")
    write_to_parquet(valid_df, epoch_id, f"{PARQUET_OUTPUT}/fact_rides")
    write_to_dlq(invalid_df, epoch_id, "ride_events")

ride_sink = (
    ride_out.writeStream
    .foreachBatch(process_rides)
    .option("checkpointLocation", "checkpoint/ride_sink")
    .trigger(processingTime="5 seconds")
    .queryName("ride_sink")
    .start()
)

# ══════════════════════════════════════════════════════════════════════════════
# STREAM 2 — DRIVER EVENTS → Validate → DLQ + PostgreSQL + Parquet
# Star Schema: fact_driver_locations
# ══════════════════════════════════════════════════════════════════════════════

driver_out = driver_df.drop("event_time")

driver_console = (
    driver_out.writeStream
    .format("console")
    .outputMode("append")
    .option("checkpointLocation", "checkpoint/driver_console")
    .trigger(processingTime="5 seconds")
    .queryName("driver_console")
    .start()
)

def process_drivers(df, epoch_id):
    """Process driver events — validate, write valid to storage, invalid to DLQ."""
    valid_df, invalid_df = validate_driver(df)
    write_to_postgres(valid_df, epoch_id, "fact_driver_locations")
    write_to_parquet(valid_df, epoch_id, f"{PARQUET_OUTPUT}/fact_driver_locations")
    write_to_dlq(invalid_df, epoch_id, "driver_events")

driver_sink = (
    driver_out.writeStream
    .foreachBatch(process_drivers)
    .option("checkpointLocation", "checkpoint/driver_sink")
    .trigger(processingTime="5 seconds")
    .queryName("driver_sink")
    .start()
)

# ══════════════════════════════════════════════════════════════════════════════
# STREAM 3 — SURGE PRICING → PostgreSQL + Parquet
# Star Schema: fact_surge_pricing
# ══════════════════════════════════════════════════════════════════════════════

surge_df = (
    ride_df
    .withWatermark("event_time", "30 seconds")
    .groupBy(window(col("event_time"), "2 minutes"))
    .agg(count("ride_id").alias("ride_count"))
    .withColumn(
        "surge_multiplier",
        when(col("ride_count") >= 10, 2.5)
        .when(col("ride_count") >= 6,  2.0)
        .when(col("ride_count") >= 3,  1.5)
        .otherwise(1.0)
    )
    .withColumn(
        "pricing_status",
        when(col("surge_multiplier") >= 2.5, "PEAK SURGE")
        .when(col("surge_multiplier") >= 2.0, "HIGH SURGE")
        .when(col("surge_multiplier") >= 1.5, "MODERATE SURGE")
        .otherwise("NORMAL")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("ride_count"),
        col("surge_multiplier"),
        col("pricing_status")
    )
)

surge_console = (
    surge_df.writeStream
    .format("console")
    .outputMode("update")
    .option("checkpointLocation", "checkpoint/surge_console")
    .option("truncate", False)
    .trigger(processingTime="10 seconds")
    .queryName("surge_console")
    .start()
)

def process_surge(df, epoch_id):
    """Process surge pricing windows — write to PostgreSQL and Parquet."""
    write_to_postgres(df, epoch_id, "fact_surge_pricing")
    write_to_parquet(df, epoch_id, f"{PARQUET_OUTPUT}/fact_surge_pricing")

surge_sink = (
    surge_df.writeStream
    .foreachBatch(process_surge)
    .outputMode("update")
    .option("checkpointLocation", "checkpoint/surge_sink")
    .trigger(processingTime="10 seconds")
    .queryName("surge_sink")
    .start()
)

# ══════════════════════════════════════════════════════════════════════════════
# STREAM 4 — DRIVER ZONES → PostgreSQL + Parquet
# Star Schema: fact_zone_demand + dim_zones
# ══════════════════════════════════════════════════════════════════════════════

zone_df = (
    ride_df
    .withWatermark("event_time", "30 seconds")
    .withColumn("lat_cell",  floor(col("pickup_lat")  / lit(GRID_SIZE)))
    .withColumn("long_cell", floor(col("pickup_long") / lit(GRID_SIZE)))
    .withColumn(
        "zone_id",
        concat_ws("_", lit("Z"),
                  col("lat_cell").cast(StringType()),
                  col("long_cell").cast(StringType()))
    )
    .withColumn(
        "zone_center_lat",
        spark_round(col("lat_cell") * lit(GRID_SIZE) + lit(GRID_SIZE / 2), 4)
    )
    .withColumn(
        "zone_center_long",
        spark_round(col("long_cell") * lit(GRID_SIZE) + lit(GRID_SIZE / 2), 4)
    )
    .groupBy("zone_id", "zone_center_lat", "zone_center_long")
    .agg(count("ride_id").alias("ride_demand"))
    .withColumn(
        "recommendation",
        when(col("ride_demand") >= 5, "HIGH DEMAND - Head here now")
        .when(col("ride_demand") >= 3, "MODERATE - Worth moving here")
        .otherwise("LOW - Monitor zone")
    )
    .orderBy(desc("ride_demand"))
)

zone_console = (
    zone_df.writeStream
    .format("console")
    .outputMode("complete")
    .option("checkpointLocation", "checkpoint/zone_console")
    .option("truncate", False)
    .trigger(processingTime="10 seconds")
    .queryName("zone_console")
    .start()
)

def process_zones(df, epoch_id):
    """Process driver zone demand — write to PostgreSQL and Parquet."""
    write_to_postgres(df, epoch_id, "fact_zone_demand")
    write_to_parquet(df, epoch_id, f"{PARQUET_OUTPUT}/fact_zone_demand")

zone_sink = (
    zone_df.writeStream
    .foreachBatch(process_zones)
    .outputMode("complete")
    .option("checkpointLocation", "checkpoint/zone_sink")
    .trigger(processingTime="10 seconds")
    .queryName("zone_sink")
    .start()
)

# ══════════════════════════════════════════════════════════════════════════════
# KEEP ALL STREAMS ALIVE
# ══════════════════════════════════════════════════════════════════════════════

logger.info("=" * 65)
logger.info("  8 ACTIVE STREAMS (4 console + 4 sinks):")
logger.info("  [1] fact_rides              → DLQ + PostgreSQL + Parquet")
logger.info("  [2] fact_driver_locations   → DLQ + PostgreSQL + Parquet")
logger.info("  [3] fact_surge_pricing      → PostgreSQL + Parquet")
logger.info("  [4] fact_zone_demand        → PostgreSQL + Parquet")
logger.info("  Spark UI: http://localhost:4040")
logger.info("=" * 65)

spark.streams.awaitAnyTermination()