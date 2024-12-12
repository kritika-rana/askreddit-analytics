-- Create external table for Reddit questions
CREATE EXTERNAL TABLE kritikarana_reddit_questions_csv (
    id STRING,
    text STRING,
    votes BIGINT,
    `timestamp` DOUBLE,    -- Using backticks to escape the reserved keyword
    datetime STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ";",
    "quoteChar" = "\""
)
STORED AS TEXTFILE
LOCATION 'wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/home/sshuser/kritikarana/project/inputs/questions'
TBLPROPERTIES ("skip.header.line.count"="1");  -- Skip the header row

CREATE TABLE kritikarana_reddit_questions (
    id STRING,
    text STRING,
    votes BIGINT,
    `timestamp` DOUBLE,    -- Using backticks to escape the reserved keyword
    datetime STRING
)
STORED AS ORC;

-- Insert data from CSV to ORC table
INSERT OVERWRITE TABLE kritikarana_reddit_questions
SELECT 
    id,
    text,
    votes,
    `timestamp`,
    datetime
FROM kritikarana_reddit_questions_csv
WHERE id IS NOT NULL      -- Filter out rows with null id
AND text IS NOT NULL;    -- Filter out rows with null text
