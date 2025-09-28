from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

from to_redis import to_redis  

# 1. Spark 세션 생성
spark = SparkSession.builder \
    .appName("CoinStreamJob") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Kafka 스트리밍 데이터 읽기
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "coin_ticker") \
    .option("startingOffsets", "latest") \
    .option("kafka.group.id", "coin_consumer_group") \
    .load()

# 3. JSON Schema 정의
schema = StructType([
    StructField("type", StringType()),
    StructField("code", StringType()),
    StructField("trade_price", DoubleType()),
    StructField("acc_trade_volume_24h", DoubleType()),
    StructField("change_rate", DoubleType()),
    StructField("signed_change_rate", DoubleType()),
    StructField("acc_trade_price_24h", DoubleType()),
    StructField("high_price", DoubleType()),
    StructField("low_price", DoubleType()),
    StructField("timestamp", LongType()),
    StructField("trade_timestamp", LongType()),
    StructField("trade_volume", DoubleType()),
])

# 4. Kafka 메시지(JSON) 파싱
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5. Redis로 적재 (foreachBatch)
query = parsed_df.writeStream \
    .foreachBatch(to_redis) \
    .outputMode("append") \
    .start()

# 6. 깔끔한 종료 처리
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Streaming job stopped by user")
    query.stop()
    spark.stop()