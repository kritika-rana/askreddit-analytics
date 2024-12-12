-- Create external table for Reddit answers
CREATE EXTERNAL TABLE kritikarana_reddit_answers_csv (
   answer_id BIGINT,
   q_id STRING, 
   text STRING,
   votes DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ";",
   "quoteChar" = "\""
)
STORED AS TEXTFILE
LOCATION 'wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/home/sshuser/kritikarana/project/inputs/answers'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Create ORC table for Reddit answers
CREATE TABLE kritikarana_reddit_answers (
   answer_id BIGINT,  -- Added answer_id
   q_id STRING,
   text STRING, 
   votes BIGINT
)
STORED AS ORC;

-- Insert data from CSV to ORC table 
INSERT OVERWRITE TABLE kritikarana_reddit_answers
SELECT
   answer_id,
   q_id,
   text,
   CAST(votes AS BIGINT) as votes
FROM kritikarana_reddit_answers_csv
WHERE q_id IS NOT NULL
AND text IS NOT NULL;
