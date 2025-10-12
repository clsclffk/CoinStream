import redis

# Redis 연결 (컨테이너 네트워크에서 접근)
r = redis.Redis(host='redis', port=6379, db=0)

def ensure_timeseries(key):
    """해당 TS key가 없으면 DUPLICATE_POLICY=LAST로 생성"""
    if not r.exists(key):
        r.execute_command("TS.CREATE", key, "DUPLICATE_POLICY", "LAST")

def to_redis(batch_df, batch_id):
    """스트리밍 배치를 Redis로 저장"""
    try:
        # collect() 대신 안전한 iterator 사용
        for row in batch_df.toLocalIterator():
            code = row["code"]
            trade_price = row["trade_price"]
            timestamp = int(row["trade_timestamp"])

            acc_trade_volume_24h = row["acc_trade_volume_24h"] or 0.0
            change_rate = row["signed_change_rate"] or 0.0
            high_price = row["high_price"] or 0.0
            low_price = row["low_price"] or 0.0
            trade_volume = row["trade_volume"] or 0.0
            trade_value = acc_trade_volume_24h * trade_price

            # Redis 파이프라인으로 성능 향상
            pipe = r.pipeline(transaction=False)

            # TS 보장
            ensure_timeseries(f"ts:{code}:price")
            ensure_timeseries(f"ts:{code}:change_rate")
            ensure_timeseries(f"ts:{code}:trade_volume")
            ensure_timeseries(f"ts:{code}:acc_trade_volume_24h")

            # TS 적재
            pipe.execute_command("TS.ADD", f"ts:{code}:price", timestamp, trade_price)
            pipe.execute_command("TS.ADD", f"ts:{code}:change_rate", timestamp, change_rate)
            pipe.execute_command("TS.ADD", f"ts:{code}:trade_volume", timestamp, trade_volume)
            pipe.execute_command("TS.ADD", f"ts:{code}:acc_trade_volume_24h", timestamp, acc_trade_volume_24h)

            # 해시 적재 (스냅샷)
            pipe.hset(f"coin:{code}", mapping={
                "code": code,
                "trade_price": trade_price,
                "timestamp": timestamp,
                "acc_trade_volume_24h": acc_trade_volume_24h,
                "change_rate": change_rate,
                "high_price": high_price,
                "low_price": low_price,
                "trade_volume": trade_volume,
            })

            # 랭킹 (ZSet)
            pipe.zadd("rank:volume_24h", {code: acc_trade_volume_24h})
            pipe.zadd("rank:change_rate", {code: change_rate})
            pipe.zadd("rank:trade_value_24h", {code: trade_value})

            # 커밋 (한 번에 Redis로 전송)
            pipe.execute()

        print(f"[Batch {batch_id}] Redis write completed")

    except Exception as e:
        print(f"[Batch {batch_id}] Redis write failed - {e}")