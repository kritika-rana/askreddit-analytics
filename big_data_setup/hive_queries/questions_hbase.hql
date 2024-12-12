CREATE EXTERNAL TABLE kritikarana_questions_hbase (
    id STRING,
    text STRING,
    votes BIGINT,
    datetime STRING,
    sentiment_score DOUBLE,
    avg_answer_sentiment DOUBLE,
    sentiment_stddev DOUBLE,
    highly_positive_count BIGINT,
    positive_count BIGINT,
    neutral_count BIGINT,
    negative_count BIGINT,
    highly_negative_count BIGINT,
    total_answers BIGINT
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = 
    ":key,metadata:text,engagement:votes#b,metadata:datetime,sentiment:score#b,answer:avg_sentiment#b,answer:stddev#b,answer:highly_positive#b,answer:positive#b,answer:neutral#b,answer:negative#b,answer:highly_negative#b,engagement:total_answers#b"
)
TBLPROPERTIES ("hbase.table.name" = "kritikarana_questions_hbase");

-- Insert data
INSERT OVERWRITE TABLE kritikarana_questions_hbase
SELECT 
    q.id,
    q.text,
    q.votes,
    q.datetime,
    q.sentiment_score,
    s.avg_answer_sentiment,
    s.sentiment_stddev,
    s.highly_positive_count,
    s.positive_count,
    s.neutral_count,
    s.negative_count,
    s.highly_negative_count,
    s.total_answers
FROM kritikarana_questions_with_sentiments q
LEFT JOIN kritikarana_question_stats s ON q.id = s.question_id;
