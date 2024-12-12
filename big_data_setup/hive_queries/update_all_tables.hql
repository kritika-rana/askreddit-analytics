-- Merge stats (update existing and add new)
MERGE INTO kritikarana_question_stats target
USING (
    SELECT
       q.id AS question_id,
       AVG(a.sentiment_score) AS avg_answer_sentiment,
       STDDEV(a.sentiment_score) AS sentiment_stddev,
       COUNT(CASE WHEN a.sentiment_score > 0.5 THEN 1 END) AS highly_positive_count,
       COUNT(CASE WHEN a.sentiment_score BETWEEN 0.05 AND 0.5 THEN 1 END) AS positive_count,
       COUNT(CASE WHEN a.sentiment_score BETWEEN -0.05 AND 0.05 THEN 1 END) AS neutral_count,
       COUNT(CASE WHEN a.sentiment_score BETWEEN -0.5 AND -0.05 THEN 1 END) AS negative_count,
       COUNT(CASE WHEN a.sentiment_score < -0.5 THEN 1 END) AS highly_negative_count,
       COUNT(CASE WHEN a.sentiment_score IS NOT NULL THEN 1 END) AS total_answers
    FROM
       kritikarana_questions_with_sentiments_latest q
    LEFT JOIN
       kritikarana_answers_with_sentiments_latest a
    ON
       q.id = a.q_id
    GROUP BY
       q.id
) source
ON target.question_id = source.question_id
WHEN MATCHED THEN UPDATE SET
    avg_answer_sentiment = source.avg_answer_sentiment,
    sentiment_stddev = source.sentiment_stddev,
    highly_positive_count = source.highly_positive_count,
    positive_count = source.positive_count,
    neutral_count = source.neutral_count,
    negative_count = source.negative_count,
    highly_negative_count = source.highly_negative_count,
    total_answers = source.total_answers
WHEN NOT MATCHED THEN INSERT VALUES
    (source.question_id, source.avg_answer_sentiment, source.sentiment_stddev,
     source.highly_positive_count, source.positive_count, source.neutral_count,
     source.negative_count, source.highly_negative_count, source.total_answers);




-- Update questions HBase
INSERT OVERWRITE TABLE kritikarana_questions_hbase
SELECT
    q.id,
    q.text,
    q.votes,
    CAST(q.datetime AS STRING),
    q.sentiment_score,
    s.avg_answer_sentiment,
    s.sentiment_stddev,
    s.highly_positive_count,
    s.positive_count,
    s.neutral_count,
    s.negative_count,
    s.highly_negative_count,
    s.total_answers
FROM (
    SELECT id, text, votes, CAST(datetime AS STRING) as datetime, sentiment_score 
    FROM kritikarana_questions_with_sentiments_latest
    UNION ALL
    SELECT * FROM kritikarana_questions_with_sentiments
) q
LEFT JOIN kritikarana_question_stats s ON q.id = s.question_id;


-- Update trending questions
INSERT OVERWRITE TABLE kritikarana_trending_questions_hbase
SELECT id, votes FROM (
    SELECT id, votes FROM kritikarana_trending_questions_hbase
    UNION ALL
    SELECT id, votes FROM kritikarana_questions_with_sentiments_latest
) combined
ORDER BY votes DESC
LIMIT 5;


-- Update answers HBase
INSERT OVERWRITE TABLE kritikarana_answers_hbase
SELECT 
    CONCAT(q_id, ':', answer_id),
    text,
    votes,
    sentiment_score
FROM (
    SELECT * FROM kritikarana_answers_with_sentiments_latest
    UNION ALL
    SELECT * FROM kritikarana_answers_with_sentiments
) combined;
