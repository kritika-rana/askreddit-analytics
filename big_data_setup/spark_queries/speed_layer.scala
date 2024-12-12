import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.hortonworks.hwc.HiveWarehouseSession
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR
import net.github.ziyasal.sentiment.SentimentIntensityAnalyzer
import org.apache.spark.sql.expressions.Window
import net.github.ziyasal.sentiment.SentimentIntensityAnalyzer

object StreamReddit {
  // Schema definitions
  val questionSchema = StructType(Seq(
    StructField("id", StringType, false),
    StructField("text", StringType, false),
    StructField("votes", LongType, false),
    StructField("datetime", TimestampType, false),
    StructField("is_update", BooleanType, false)
  ))

  val answerSchema = StructType(Seq(
    StructField("answer_id", StringType, false),
    StructField("q_id", StringType, false),
    StructField("text", StringType, false),
    StructField("votes", LongType, false),
    StructField("is_update", BooleanType, false)
  ))

  // Sentiment Analysis UDF
  val sentimentUDF = udf((text: String) => {
    val analyzer = new SentimentIntensityAnalyzer()
    analyzer.polarityScores(text).compound
  })

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: RedditProcessor <kafka_brokers>")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("RedditProcessor")
      .enableHiveSupport()
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(300)) // 5 minutes

    // Set up HiveWarehouseSession
    val hive = HiveWarehouseSession.session(spark).build()
    hive.setDatabase("default")

    import spark.implicits._

    // Create Hive tables with proper schema
    hive.createTable("kritikarana_questions_with_sentiments_latest")
      .ifNotExists()
      .column("id", "string")
      .column("text", "string")
      .column("votes", "bigint")
      .column("datetime", "timestamp")
      .column("sentiment_score", "double")
      .create()

    hive.createTable("kritikarana_answers_with_sentiments_latest")
      .ifNotExists()
      .column("answer_id", "string")
      .column("q_id", "string")
      .column("text", "string")
      .column("votes", "bigint")
      .column("sentiment_score", "double")
      .create()

    // Kafka Configuration
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> args(0),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "reddit_processor",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // Create streams
    val questionsStream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](Set("kritikarana-reddit-questions"), kafkaParams)
    )

    val answersStream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](Set("kritikarana-reddit-answers"), kafkaParams)
    )

    // Process questions
    questionsStream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        // Process questions into DataFrame
        val questionsDF = spark.read
          .schema(questionSchema)
          .json(rdd.map(_.value()))
          .withColumn("processed_at", current_timestamp())
          .withColumn("sentiment_score", sentimentUDF(col("text")))

        val window = Window.partitionBy("id").orderBy(col("processed_at").desc)

        val latestQuestions = questionsDF
          .withColumn("row_num", row_number().over(window))
          .where(col("row_num") === 1)
          .drop("row_num", "processed_at", "is_update")
          .select("id", "text", "votes", "datetime", "sentiment_score")

        // Create temporary Hive table
        hive.executeUpdate("DROP TABLE IF EXISTS kritikarana_latest_questions_temp")
        hive.createTable("kritikarana_latest_questions_temp")
          .column("id", "string")
          .column("text", "string")
          .column("votes", "bigint")
          .column("datetime", "timestamp")
          .column("sentiment_score", "double")
          .create()

        // Write data to temp table
        latestQuestions
          .write
          .format(HIVE_WAREHOUSE_CONNECTOR)
          .mode("overwrite")
          .option("table", "kritikarana_latest_questions_temp")
          .save()

        // Perform merge operation
        hive.executeUpdate("""
      MERGE INTO kritikarana_questions_with_sentiments_latest target
      USING kritikarana_latest_questions_temp source
      ON target.id = source.id
      WHEN MATCHED THEN UPDATE SET
        text = source.text,
        votes = source.votes,
        datetime = source.datetime,
        sentiment_score = source.sentiment_score
      WHEN NOT MATCHED THEN INSERT VALUES
        (source.id, source.text, source.votes, source.datetime, source.sentiment_score)
      """)

        // Clean up temp table
        hive.executeUpdate("DROP TABLE IF EXISTS kritikarana_latest_questions_temp")

        // Commit Kafka offsets
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        questionsStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    }

    // Process answers
    answersStream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        // Process answers into DataFrame
        val answersDF = spark.read
          .schema(answerSchema)
          .json(rdd.map(_.value()))
          .withColumn("processed_at", current_timestamp())
          .withColumn("sentiment_score", sentimentUDF(col("text")))

        val window = Window.partitionBy("answer_id").orderBy(col("processed_at").desc)

        val latestAnswers = answersDF
          .withColumn("row_num", row_number().over(window))
          .where(col("row_num") === 1)
          .drop("row_num", "processed_at", "is_update")
          .select("answer_id", "q_id", "text", "votes", "sentiment_score")

        // Create temporary Hive table
        hive.executeUpdate("DROP TABLE IF EXISTS kritikarana_latest_answers_temp")
        hive.createTable("kritikarana_latest_answers_temp")
          .column("answer_id", "string")
          .column("q_id", "string")
          .column("text", "string")
          .column("votes", "bigint")
          .column("sentiment_score", "double")
          .create()

        // Write data to temp table
        latestAnswers
          .write
          .format(HIVE_WAREHOUSE_CONNECTOR)
          .mode("overwrite")
          .option("table", "kritikarana_latest_answers_temp")
          .save()

        // Perform merge operation
        hive.executeUpdate("""
      MERGE INTO kritikarana_answers_with_sentiments_latest target
      USING kritikarana_latest_answers_temp source
      ON target.answer_id = source.answer_id
      WHEN MATCHED THEN UPDATE SET
        q_id = source.q_id,
        text = source.text,
        votes = source.votes,
        sentiment_score = source.sentiment_score
      WHEN NOT MATCHED THEN INSERT VALUES
        (source.answer_id, source.q_id, source.text, source.votes, source.sentiment_score)
      """)

        // Clean up temp table
        hive.executeUpdate("DROP TABLE IF EXISTS kritikarana_latest_answers_temp")

        // Commit Kafka offsets
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        answersStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
