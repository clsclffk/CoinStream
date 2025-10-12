from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.streaming import StreamingQueryListener
from prometheus_client import Gauge, start_http_server
from to_redis import to_redis

# =============================
# 1. Prometheus Exporter 설정
# =============================
# Prometheus에서 긁을 지표 정의
input_rate = Gauge("spark_streaming_inputRowsPerSecond", "Kafka → Spark 유입 속도")
processed_rate = Gauge("spark_streaming_processedRowsPerSecond", "Spark 처리 속도")
batch_latency = Gauge("spark_streaming_batchLatencyMs", "Spark 배치 처리 지연 (ms)")

# Structured Streaming 이벤트를 받아 메트릭 업데이트
class PrometheusStreamingListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Query started: {event.id}")

    def onQueryProgress(self, event):
        p = event.progress
        input_rate.set(p.inputRowsPerSecond or 0.0)
        processed_rate.set(p.processedRowsPerSecond or 0.0)
        batch_latency.set(p.durationMs.get("batchDuration", 0))

    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")

    def onQueryIdle(self, event):
        pass

# Prometheus Exporter 서버 시작 (8099 포트로 노출)
start_http_server(8099)
print("Prometheus metrics exporter started on port 8099")

# =============================
# 2. Spark 세션 생성
# =============================
spark = SparkSession.builder \
    .appName("CoinStreamJob") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
spark.streams.addListener(PrometheusStreamingListener())  # Listener 등록

# =============================
# 3. Kafka 스트리밍 데이터 읽기
# =============================
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "coin_ticker") \
    .option("startingOffsets", "latest") \
    .load()

# =============================
# 4. JSON 스키마 정의 및 파싱
# =============================
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

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# =============================
# 5. Redis로 적재 (foreachBatch)
# =============================
query = parsed_df.writeStream \
    .trigger(processingTime="5 seconds") \
    .foreachBatch(to_redis) \
    .outputMode("append") \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Streaming job stopped by user")
    query.stop()
    spark.stop()
