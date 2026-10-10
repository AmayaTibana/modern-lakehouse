import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.ingest.common import LOCAL_ROOT, S3_BUCKET, STORAGE_MODE, today_partition


def base_path() -> str:
    return f"s3a://{S3_BUCKET}" if STORAGE_MODE == "s3" else str(LOCAL_ROOT)


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("lakehouse-transform")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    )
    if STORAGE_MODE == "s3":
        builder = builder.config(
            "spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4"
        ).config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    return builder.getOrCreate()


def transform_pokeapi(spark: SparkSession, base: str, dt: str) -> None:
    raw = spark.read.json(f"{base}/raw/pokeapi/dt={dt}/pokemon_*.json").dropDuplicates(["id"])

    stats = (
        raw.select("id", F.explode("stats").alias("s"))
        .select("id", F.col("s.stat.name").alias("stat_name"), F.col("s.base_stat").alias("base_stat"))
        .groupBy("id")
        .pivot("stat_name")
        .agg(F.first("base_stat"))
    )
    for column in stats.columns:
        stats = stats.withColumnRenamed(column, column.replace("-", "_"))

    pokemon = raw.select("id", "name", "height", "weight", "base_experience").join(stats, "id")

    types = (
        raw.select(F.col("id").alias("pokemon_id"), F.explode("types").alias("t"))
        .select("pokemon_id", F.col("t.slot").alias("slot"), F.col("t.type.name").alias("type_name"))
    )

    pokemon.write.mode("overwrite").parquet(f"{base}/curated/pokeapi/pokemon")
    types.write.mode("overwrite").parquet(f"{base}/curated/pokeapi/pokemon_types")
    print(f"pokeapi: {pokemon.count()} pokemon, {types.count()} type rows")


def transform_weather(spark: SparkSession, base: str, dt: str) -> None:
    raw = spark.read.json(f"{base}/raw/weather/dt={dt}/*_forecast.json")

    periods = raw.select(
        "location",
        "latitude",
        "longitude",
        "ingested_at",
        F.explode("forecast.properties.periods").alias("p"),
    )
    forecasts = periods.select(
        "location",
        "latitude",
        "longitude",
        "ingested_at",
        F.lit(dt).cast("date").alias("ingest_date"),
        F.col("p.number").alias("period_number"),
        F.col("p.name").alias("period_name"),
        F.to_timestamp("p.startTime").alias("start_time"),
        F.to_timestamp("p.endTime").alias("end_time"),
        F.col("p.temperature").alias("temperature"),
        F.col("p.temperatureUnit").alias("temperature_unit"),
        F.col("p.isDaytime").alias("is_daytime"),
        F.col("p.windSpeed").alias("wind_speed"),
        F.col("p.windDirection").alias("wind_direction"),
        F.col("p.shortForecast").alias("short_forecast"),
        F.col("p.detailedForecast").alias("detailed_forecast"),
    )

    forecasts.write.mode("overwrite").parquet(f"{base}/curated/weather/forecasts/ingest_date={dt}")
    print(f"weather: {forecasts.count()} forecast periods")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["pokeapi", "weather", "all"], default="all")
    parser.add_argument("--dt", default=today_partition())
    args = parser.parse_args()

    spark = build_spark()
    base = base_path()
    if args.source in ("pokeapi", "all"):
        transform_pokeapi(spark, base, args.dt)
    if args.source in ("weather", "all"):
        transform_weather(spark, base, args.dt)
    spark.stop()