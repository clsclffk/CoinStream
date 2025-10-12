from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, min, max, first, last, sum, from_unixtime
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta, timezone

"""
Kafka → Spark → S3 (전일 로그 배치 적재)

- Kafka 토픽: coin_ticker
- 적재 기간: 전일 00:00 ~ 오늘 00:00 (KST)
- 집계 단위: 1분봉 (OHLCV)
- 결과 저장: AWS S3 (Parquet 형식, partitionBy(code, date))
"""

def create_spark_session():
    load_dotenv()
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_REGION")

    spark = SparkSession.builder \
        .appName("KafkaBatchToS3") \
        .getOrCreate()

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", access_key)
    hadoop_conf.set("fs.s3a.secret.key", secret_key)
    hadoop_conf.set("fs.s3a.endpoint", f"s3.{region}.amazonaws.com")
    hadoop_conf.set("fs.s3a.path.style.access", "true")

    return spark


def batch_to_s3():
    spark = create_spark_session()

    schema = StructType([
        StructField("code", StringType()),
        StructField("trade_price", DoubleType()),
        StructField("trade_volume", DoubleType()),
        StructField("trade_timestamp", LongType())
    ])

    # === 전일 날짜 범위 (KST 기준) ===
    KST = timezone(timedelta(hours=9))

    today_kst = datetime.now(KST).date()
    yesterday_kst = today_kst - timedelta(days=1)

    # 전일 00:00 ~ 오늘 00:00 구간
    start_ts = int(datetime.combine(yesterday_kst, datetime.min.time(), tzinfo=KST).timestamp() * 1000)
    end_ts = int(datetime.combine(today_kst, datetime.min.time(), tzinfo=KST).timestamp() * 1000)

    print(">>> KST start:", start_ts,
        "KST:", datetime.fromtimestamp(start_ts/1000, tz=KST),
        "UTC:", datetime.utcfromtimestamp(start_ts/1000))

    print(">>> KST end:", end_ts,
        "KST:", datetime.fromtimestamp(end_ts/1000, tz=KST),
        "UTC:", datetime.utcfromtimestamp(end_ts/1000))

    # Kafka → 배치 읽기
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "coin_ticker") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
    print(">>> Raw Kafka count:", df.count())

    # JSON 파싱
    df_parsed = df.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*")
    print(">>> Parsed count:", df_parsed.count())

    # epoch(ms) → timestamp 변환
    df_parsed = df_parsed.withColumn(
        "event_time",
        from_unixtime(col("trade_timestamp") / 1000).cast("timestamp")
    )

    print(">>> Debug: start_ts (ms):", start_ts, "UTC:", datetime.utcfromtimestamp(start_ts/1000))
    print(">>> Debug: end_ts   (ms):", end_ts, "UTC:", datetime.utcfromtimestamp(end_ts/1000))

    # 샘플 몇 줄만 출력 (show 대신 collect 사용)
    sample_rows = df_parsed.select(
        "code",
        "trade_timestamp",
        from_unixtime(col("trade_timestamp")/1000).alias("event_time_utc")
    ).limit(5).collect()

    for row in sample_rows:
        print(">>> Sample row:", row)


    # 전일 데이터만 필터링
    df_filtered = df_parsed.filter(
        (col("trade_timestamp") >= start_ts) & (col("trade_timestamp") < end_ts)
    )
    print(">>> Filtered count (yesterday only):", df_filtered.count())

    # 1분봉 집계
    df_candle = df_filtered.groupBy(
        col("code"),
        window(col("event_time"), "1 minute")
    ).agg(
        first("trade_price").alias("open"),
        max("trade_price").alias("high"),
        min("trade_price").alias("low"),
        last("trade_price").alias("close"),
        sum("trade_volume").alias("volume")
    )
    print(">>> Candle count:", df_candle.count())

    # S3 저장
    df_candle \
        .withColumn("date", col("window.start").cast("date")) \
        .write \
        .mode("append") \
        .partitionBy("code", "date") \
        .parquet("s3a://coin-stream-data/1min_candles/")

    spark.stop()


if __name__ == "__main__":
    batch_to_s3()
