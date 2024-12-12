CREATE TABLE kritikarana_question_stats (
   question_id STRING,
   avg_answer_sentiment DOUBLE,
   sentiment_stddev DOUBLE,
   highly_positive_count BIGINT,
   positive_count BIGINT,
   neutral_count BIGINT,
   negative_count BIGINT,
   highly_negative_count BIGINT,
   total_answers BIGINT
)
STORED AS ORC;

INSERT OVERWRITE TABLE kritikarana_question_stats
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
   kritikarana_questions_with_sentiments q
LEFT JOIN
   kritikarana_answers_with_sentiments a
ON
   q.id = a.q_id
GROUP BY
   q.id;
