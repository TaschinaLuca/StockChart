CREATE OR REPLACE STAGE finance_project.analytics.kafka_stage 
    FILE_FORMAT = (TYPE = 'JSON');

CREATE OR REPLACE TABLE finance_project.analytics.raw_stock_feed (
    stock_name TEXT,
    stock_low FLOAT,
    stock_high FLOAT,
    stock_date DATE,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

select * from finance_project.analytics.raw_stock_feed
TRUNCATE TABLE finance_project.analytics.raw_stock_feed;
REMOVE @finance_project.analytics.kafka_stage;