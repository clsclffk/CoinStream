# kafka -> spark 연결 테스트
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# 1. Spark 세션 생성
spark = SparkSession.builder \
    .appName("CoinStreamTest") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Kafka 스트리밍 데이터 읽기
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "coin_ticker") \
    .option("startingOffsets", "latest") \
    .load()

# 3. JSON Schema 정의
schema = StructType([
    StructField("type", StringType()),
    StructField("code", StringType()),
    StructField("trade_price", DoubleType()),
    StructField("acc_trade_volume", DoubleType()),
    StructField("timestamp", LongType())
])

# 4. Kafka 메시지 → JSON 변환
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5. 콘솔에 출력 (테스트용)
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()

"""
# spark에서 실행
docker exec -it coin_stream-spark-master-1 spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/bitnami/spark/scripts/spark_test.py

# 출력 예시
+------+-------+-----------+----------------+-------------+
|type  |code   |trade_price|acc_trade_volume|timestamp    |
+------+-------+-----------+----------------+-------------+
|ticker|KRW-BTC|1.62354E8  |516.83619663    |1758378709864|
+------+-------+-----------+----------------+-------------+
"""