import asyncio
import websockets
import json
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'kafka:9092',
}

producer = Producer(conf)

async def upbit_ws_client():
    uri = "wss://api.upbit.com/websocket/v1"
    async with websockets.connect(uri) as websocket:
        print("Upbit WebSocket 연결 성공", flush=True)
        # 여러 코인 구독 (BTC, ETH, XRP)
        subscribe_fmt = [
            {"ticket": "test"},
            {"type": "ticker", "codes": ["KRW-BTC", "KRW-ETH", "KRW-XRP"]}
        ]
        await websocket.send(json.dumps(subscribe_fmt))
        print("KRW-BTC, KRW-ETH, KRW-XRP ticker 구독 요청 전송 완료", flush=True)

        while True:
            data = await websocket.recv()
            try:
                response = json.loads(data.decode('utf-8'))
                print("[DEBUG 전체 데이터]", response, flush=True)

                producer.produce("coin_ticker", json.dumps(response).encode("utf-8"))
                producer.flush()
                print(f"[Kafka 전송] {response['code']} -> {response['trade_price']}")
            except Exception as e:
                print("디코딩 에러:", e)

if __name__ == "__main__":
    asyncio.run(upbit_ws_client())
