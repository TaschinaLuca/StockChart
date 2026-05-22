import os
import json
import snowflake.connector
from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()

# Connect to Snowflake
print("Connecting to Snowflake...")
ctx = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA')
)
cursor = ctx.cursor()

# Connect to Local Kafka
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'ultimate_stock_group_v1',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['stock_live_feed'])

BATCH_SIZE = 50
message_batch = []

print("🌉 Bridge online. Listening for Kafka messages...")

try:
    while True:
        msg = consumer.poll(1.0)
        
        if msg is None: continue
        if msg.error():
            print(f"Kafka Error: {msg.error()}")
            continue

        raw_json = msg.value().decode('utf-8')
        message_batch.append(raw_json)
        print(f"Buffered message {len(message_batch)}/{BATCH_SIZE}")

        if len(message_batch) >= BATCH_SIZE:
            print("Batch full! Uploading to Snowflake...")
            
            file_name = "temp_batch.json"
            with open(file_name, "w") as f:
                for record in message_batch:
                    f.write(record + "\n")
            
            cursor.execute(f"PUT file://{file_name} @kafka_stage AUTO_COMPRESS=TRUE")
            
            cursor.execute("""
                COPY INTO finance_project.analytics.raw_stock_feed 
                    (stock_name, stock_low, stock_high, stock_date) 
                FROM (
                    SELECT 
                        $1:name::VARCHAR,
                        $1:low::FLOAT,
                        $1:high::FLOAT,
                        $1:date::DATE
                    FROM @finance_project.analytics.kafka_stage
                )
                FILE_FORMAT = (TYPE = 'JSON')
                PURGE = TRUE
            """)
            
            print("✅ Batch successfully loaded into Snowflake!")
            
            os.remove(file_name)
            message_batch.clear()

except KeyboardInterrupt:
    print("\nShutting down bridge...")
finally:
    consumer.close()
    cursor.close()
    ctx.close()