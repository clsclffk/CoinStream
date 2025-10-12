import asyncio
import websockets
import json
import requests
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'kafka:9092',
}

producer = Producer(conf)

def get_all_krw_markets():
    url = "https://api.upbit.com/v1/market/all"
    res = requests.get(url)
    markets = res.json()
    krw_markets = [m["market"] for m in markets if m["market"].startswith("KRW-")]
    print(f"[INFO] 구독할 KRW 코인 수: {len(krw_markets)}개")
    return krw_markets

# WebSocket 클라이언트
async def upbit_ws_client():
    uri = "wss://api.upbit.com/websocket/v1"
    codes = get_all_krw_markets() 

    async with websockets.connect(uri) as websocket:
        print("[INFO] Upbit WebSocket 연결 성공", flush=True)

        subscribe_fmt = [
            {"ticket": "test"},
            {"type": "ticker", "codes": codes}  
        ]
        await websocket.send(json.dumps(subscribe_fmt))
        print("[INFO] 전체 KRW 코인 ticker 구독 요청 전송 완료", flush=True)

        while True:
            data = await websocket.recv()
            try:
                response = json.loads(data.decode('utf-8'))

                producer.produce("coin_ticker", json.dumps(response).encode("utf-8"))
                producer.flush()

                print(f"[Kafka 전송] {response['code']} -> {response['trade_price']}")
            except Exception as e:
                print("[ERROR 디코딩 실패]", e)

if __name__ == "__main__":
    asyncio.run(upbit_ws_client())