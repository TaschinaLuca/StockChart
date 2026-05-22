import json
from dotenv import load_dotenv
from confluent_kafka import Producer
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.timeframe import TimeFrame
from datetime import datetime
from alpaca.data.requests import StockBarsRequest


load_dotenv()

# Kafka setup (Local Docker Version)
kafka_config = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(kafka_config)
KAFKA_TOPIC = "stock_live_feed"

def delivery_report(err, msg):
    """Callback to confirm message was delivered."""
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to Kafka Topic [{msg.topic()}]")

# Stock Setup
stock_client = StockHistoricalDataClient(
    api_key="",
    secret_key=""
)

stock_str = "META"

request_params = StockBarsRequest(
    symbol_or_symbols=[stock_str],
    timeframe=TimeFrame.Day,
    start=datetime(2025, 4, 2),
    end=datetime(2026, 5, 22)
)

stock_bars = stock_client.get_stock_bars(request_params)

# Streaming loop
def stream_alpaca_data():
    try:
        for stock in stock_bars[stock_str]:
            stock_event = {
                "name": stock_str,
                "high": stock.high,
                "low": stock.low,
                "date": stock.timestamp.strftime('%Y-%m-%d')
            }

            producer.produce(
                topic=KAFKA_TOPIC, 
                value=json.dumps(stock_event).encode('utf-8'), 
                callback=delivery_report
            )
            
            producer.poll(0) 
            
    except Exception as e:
        print(f"Error: {e}")
        
    finally:
        print("Waiting for Kafka confirmation...")
        producer.flush()

if __name__ == "__main__":
    stream_alpaca_data()