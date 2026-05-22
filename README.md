📈 Real-Time Financial Data Pipeline

Overview
An end-to-end Data Engineering ELT (Extract, Load, Transform) pipeline built to stream, store, and visualize historical and real-time financial data. This project extracts stock data via the Alpaca API, buffers it through a local Apache Kafka broker, micro-batches it into a Snowflake Data Warehouse, and serves the analytics via an interactive Streamlit dashboard.

🏗️ Architecture & Data Flow
This pipeline is decoupled into distinct Extraction, Buffering, Loading, and Visualization layers to simulate an enterprise data architecture:

Extract (Python Producer): A Python script queries the Alpaca Market API for stock data. It structures the payload (Name, High, Low, Date) into JSON and publishes it to a specific Kafka topic.

Buffer (Dockerized Kafka): Apache Kafka runs locally via a Docker container in KRaft mode (Zookeeper-less). It acts as a highly reliable message broker, holding the streaming data in a persistent queue (reddit_live_feed / stock_feed) to prevent data loss.

Load (Python Snowflake Bridge): A custom consumer acts as an automated micro-batching engine. It reads messages from Kafka, buffers them locally, and uses the Snowflake Python Connector to PUT the batch into a Snowflake Internal Stage. It then executes a COPY INTO command to parse the JSON and map it to a strictly typed structured database table.

Transform & Visualize (Streamlit + SQL): A Streamlit front-end connects directly to Snowflake. It uses SQL to dynamically convert USD prices to Euros on the fly, filtering for Year-to-Date trends, and renders an interactive Altair chart.

🛠️ Tech Stack
Language: Python 3

Data Sources: Alpaca Trading API

Message Broker: Apache Kafka (KRaft), Docker & Docker Compose

Data Warehouse: Snowflake (Internal Stages, Snowpipe concepts, SQL)

Frontend: Streamlit, Altair, Pandas

🚀 Setup & Execution
1. Prerequisites
Docker Desktop installed and running.

A Snowflake account with a database named finance_project.

An .env file in the root directory containing your API and Database keys.

2. Start the Kafka Message Broker
Navigate to the project folder and spin up the Kafka container (configured to handle WSL2/Windows networking constraints on 127.0.0.1):

Bash
docker-compose up -d
3. Start the Pipeline
You need three separate terminal windows to run the decoupled architecture:

Terminal 1: Start the Snowflake Bridge (Consumer)
Listens to Kafka and micro-batches data into the Snowflake Warehouse.

Bash
python snowflake_bridge.py
Terminal 2: Start the Data Extractor (Producer)
Fetches Alpaca data and streams it into the Kafka queue.

Bash
python mining_script.py
Terminal 3: Launch the Dashboard
Boots the Streamlit web app to visualize the data.

Bash
streamlit run app.py
🧠 Key Learnings & Technical Challenges Overcome
Docker Networking on Windows: Overcame WSL2 localhost routing issues by forcing IPv4 (127.0.0.1) and binding Kafka listeners to 0.0.0.0.

Single-Node Kafka Configuration: Configured Kafka for local development by modifying KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR to prevent infinite restart loops when running without backup brokers.

Snowflake Schema Mapping: Progressed from raw VARIANT JSON dumps to mapping nested JSON directly into structured, strictly typed columns using Snowflake's $1 notation during the COPY INTO command.
