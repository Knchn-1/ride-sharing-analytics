from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, round as spark_round,
    when, concat_ws, floor, lit, desc
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType,
    DoubleType, StringType, TimestampType
)
import os
from dotenv import load_dotenv

# This will load the variables from your .env file into os.environ
load_dotenv()

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG  — change only these values
# ══════════════════════════════════════════════════════════════════════════════

PG_HOST     = os.getenv("PG_HOST", "localhost")
PG_PORT     = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "ride_analytics")
PG_USER     = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD")


PARQUET_OUTPUT = "D:/Data_eng_Projects/ride-sharing-analytics/data/output"

# Path to the manually downloaded PostgreSQL JDBC jar
# After running the download command, the jar will be in your project root
PG_JAR = "file:///D:/Data_eng_Projects/ride-sharing-analytics/postgresql-42.6.0.jar"

PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
PG_PROPS = {
    "user":     PG_USER,
    "password": PG_PASSWORD,
    "driver":   "org.postgresql.Driver"
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
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

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
# HELPER — write each micro-batch to PostgreSQL + Parquet
# ══════════════════════════════════════════════════════════════════════════════

def write_to_postgres(df, epoch_id, table_name):
    """Write each streaming micro-batch to a PostgreSQL table with robust property handling."""
    try:
        # Create a clean property dictionary, ensuring no None values exist
        # We explicitly cast values to string to satisfy Java's Properties class
        safe_props = {
            "user": str(PG_USER),
            "password": str(PG_PASSWORD),
            "driver": "org.postgresql.Driver"
        }
        
        row_count = df.count()
        if row_count > 0:
            df.write \
              .jdbc(url=PG_URL, table=table_name, mode="append", properties=safe_props)
            print(f"  ✅ [{table_name}] wrote {row_count} rows to PostgreSQL")
    except Exception as e:
        print(f"  ❌ [{table_name}] PostgreSQL write failed: {e}")

def write_to_parquet(df, epoch_id, path):
    """Write each streaming micro-batch to partitioned Parquet files."""
    try:
        if df.count() > 0:
            df.write \
              .mode("append") \
              .parquet(path)
    except Exception as e:
        print(f"  ❌ Parquet write failed at {path}: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# READ FROM KAFKA
# ══════════════════════════════════════════════════════════════════════════════

def read_topic(topic):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
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
# STREAM 1 — RAW RIDE EVENTS → Console + PostgreSQL + Parquet
# ══════════════════════════════════════════════════════════════════════════════

ride_out = ride_df.drop("event_time")

# Console
ride_console = (
    ride_out.writeStream
    .format("console")
    .outputMode("append")
    .option("checkpointLocation", "checkpoint/ride_console")
    .trigger(processingTime="5 seconds")
    .queryName("ride_console")
    .start()
)

# PostgreSQL + Parquet via foreachBatch
def process_rides(df, epoch_id):
    write_to_postgres(df, epoch_id, "ride_events")
    write_to_parquet(df, epoch_id, f"{PARQUET_OUTPUT}/rides")

ride_sink = (
    ride_out.writeStream
    .foreachBatch(process_rides)
    .option("checkpointLocation", "checkpoint/ride_sink")
    .trigger(processingTime="5 seconds")
    .queryName("ride_sink")
    .start()
)

# ══════════════════════════════════════════════════════════════════════════════
# STREAM 2 — RAW DRIVER EVENTS → Console + PostgreSQL + Parquet
# ══════════════════════════════════════════════════════════════════════════════

driver_out = driver_df.drop("event_time")

# Console
driver_console = (
    driver_out.writeStream
    .format("console")
    .outputMode("append")
    .option("checkpointLocation", "checkpoint/driver_console")
    .trigger(processingTime="5 seconds")
    .queryName("driver_console")
    .start()
)

# PostgreSQL + Parquet via foreachBatch
def process_drivers(df, epoch_id):
    write_to_postgres(df, epoch_id, "driver_events")
    write_to_parquet(df, epoch_id, f"{PARQUET_OUTPUT}/drivers")

driver_sink = (
    driver_out.writeStream
    .foreachBatch(process_drivers)
    .option("checkpointLocation", "checkpoint/driver_sink")
    .trigger(processingTime="5 seconds")
    .queryName("driver_sink")
    .start()
)

# ══════════════════════════════════════════════════════════════════════════════
# STREAM 3 — SURGE PRICING → Console + PostgreSQL + Parquet
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

# Console
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

# PostgreSQL + Parquet via foreachBatch
def process_surge(df, epoch_id):
    write_to_postgres(df, epoch_id, "surge_pricing")
    write_to_parquet(df, epoch_id, f"{PARQUET_OUTPUT}/surge")

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
# STREAM 4 — DRIVER ZONES → Console + PostgreSQL + Parquet
# ══════════════════════════════════════════════════════════════════════════════

GRID_SIZE = 0.02

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
        spark_round(col("lat_cell")  * lit(GRID_SIZE) + lit(GRID_SIZE / 2), 4)
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

# Console
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

# PostgreSQL + Parquet via foreachBatch
def process_zones(df, epoch_id):
    write_to_postgres(df, epoch_id, "driver_zones")
    write_to_parquet(df, epoch_id, f"{PARQUET_OUTPUT}/zones")

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

print("\n" + "=" * 65)
print("  8 ACTIVE STREAMS (4 console + 4 sinks):")
print("  [1] ride_events    → console + PostgreSQL + Parquet")
print("  [2] driver_events  → console + PostgreSQL + Parquet")
print("  [3] surge_pricing  → console + PostgreSQL + Parquet")
print("  [4] driver_zones   → console + PostgreSQL + Parquet")
print("=" * 65 + "\n")

spark.streams.awaitAnyTermination()