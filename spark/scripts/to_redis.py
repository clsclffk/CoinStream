import redis

# Redis 연결 (컨테이너 네트워크에서 접근)
r = redis.Redis(host='redis', port=6379, db=0)

def ensure_timeseries(key: str):
    """해당 TS key가 없으면 DUPLICATE_POLICY=LAST로 생성"""
    if not r.exists(key):
        r.execute_command("TS.CREATE", key, "DUPLICATE_POLICY", "LAST")

def to_redis(batch_df, batch_id):
    records = batch_df.collect()

    for row in records:
        code = row["code"]
        trade_price = row["trade_price"]
        timestamp = int(row["trade_timestamp"])

        acc_trade_volume_24h = row["acc_trade_volume_24h"] or 0.0
        change_rate = row["signed_change_rate"] or 0.0
        high_price = row["high_price"] or 0.0
        low_price = row["low_price"] or 0.0
        trade_volume = row["trade_volume"] or 0.0  

        # TS 보장
        ensure_timeseries(f"ts:{code}:price")
        ensure_timeseries(f"ts:{code}:change_rate")
        ensure_timeseries(f"ts:{code}:trade_volume")   
        ensure_timeseries(f"ts:{code}:acc_trade_volume_24h")

        # TS 적재
        r.execute_command("TS.ADD", f"ts:{code}:price", timestamp, trade_price)
        r.execute_command("TS.ADD", f"ts:{code}:change_rate", timestamp, change_rate)
        r.execute_command("TS.ADD", f"ts:{code}:trade_volume", timestamp, trade_volume)   
        r.execute_command("TS.ADD", f"ts:{code}:acc_trade_volume_24h", timestamp, acc_trade_volume_24h)

        # 해시 적재 (스냅샷)
        r.hset(f"coin:{code}", mapping={
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
        r.hset("rank:volume_24h", code, acc_trade_volume_24h)
        r.hset("rank:change_rate", code, change_rate)

        # 알람 리스트
        r.lpush("alerts:events", f"{timestamp}|{code}|{trade_price}|{change_rate}|{trade_volume}")
        r.ltrim("alerts:events", 0, 99)

        print(f"[Redis] {code} -> {trade_price} (ts + hash + zset)")
        print(f"[DEBUG] code={code}, row.timestamp={row['timestamp']}, trade_timestamp={row['trade_timestamp']}")
