CREATE EXTERNAL TABLE kritikarana_answers_hbase (
    answer_key STRING,
    text STRING,
    votes BIGINT,
    sentiment_score DOUBLE
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = 
    ":key,metadata:text,metadata:votes#b,sentiment:score#b"
)
TBLPROPERTIES ("hbase.table.name" = "kritikarana_answers_hbase");

-- Insert data
INSERT OVERWRITE TABLE kritikarana_answers_hbase
SELECT 
    CONCAT(q_id, ':', answer_id),
    text,
    votes,
    sentiment_score
FROM kritikarana_answers_with_sentiments;
