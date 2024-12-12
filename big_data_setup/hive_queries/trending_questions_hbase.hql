CREATE EXTERNAL TABLE kritikarana_trending_questions_hbase (
    id STRING,                      -- Row key for HBase
    votes BIGINT                    -- Number of upvotes
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,engagement:votes#b"  -- Map row key and votes column
)
TBLPROPERTIES (
    "hbase.table.name" = "kritikarana_trending_questions_hbase" -- HBase table name
);

INSERT OVERWRITE TABLE kritikarana_trending_questions_hbase
SELECT
    q.id AS id,                   -- Row key for HBase
    q.votes AS votes              -- Number of upvotes
FROM
    kritikarana_questions_with_sentiments q
ORDER BY
    q.votes DESC                  -- Order by most votes in descending order
LIMIT 5;                          -- Fetch only the top 5 questions

