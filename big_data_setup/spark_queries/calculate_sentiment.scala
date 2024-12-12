:require /home/sshuser/kritikarana/project/jars/sentiment_2.12-0.1.0.jar

// import com.hortonworks.hwc.HiveWarehouseSession
// import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR
import net.github.ziyasal.sentiment.SentimentIntensityAnalyzer
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

// Step 1: Initialize Hive Warehouse Session
/*
val hive = HiveWarehouseSession.session(spark).build()
hive.setDatabase("default")
*/
// we will use loadHiveTable since this is giving credential error
def loadHiveTable(tableName: String): DataFrame = {
  val prefix = s"$tableName."
  // Load the table into a DataFrame
  var df = spark.read.format("jdbc")
    .option("url", "jdbc:hive2://10.0.0.50:10001/;transportMode=http")
    .option("dbtable", tableName)
    .load()
  // Remove the prefix from each column name by renaming columns explicitly
  df.columns.foreach { colName =>
    if (colName.startsWith(prefix)) {
      df = df.withColumnRenamed(colName, colName.stripPrefix(prefix))
    }
  }
  // Return the flattened DataFrame
  df }

// Step 2: Register UDF for Sentiment Analysis
val sentimentUDF = udf((text: String) => {
  val analyzer = new SentimentIntensityAnalyzer() // Instantiate locally in the UDF
  analyzer.polarityScores(text).compound          // Compute sentiment score
})

// Step 3: Read Hive Tables
val questionsDF = loadHiveTable("kritikarana_reddit_questions")
val answersDF = loadHiveTable("kritikarana_reddit_answers")
  .withColumn("answer_id", col("answer_id").cast("string")) // Cast answer_id to string

// Step 4: Process Data (Add Sentiments)
val questionsWithSentimentsDF = questionsDF.withColumn("sentiment_score", sentimentUDF(col("text")))
val answersWithSentimentsDF = answersDF.withColumn("sentiment_score", sentimentUDF(col("text")))

// Step 5: Write Data Back to Hive
// Create "questions_with_sentiments" table
/*
hive.createTable("kritikarana_questions_with_sentiments")
  .ifNotExists()
  .column("id", "string")
  .column("text", "string")
  .column("votes", "bigint")
  .column("datetime", "string")
  .column("sentiment_score", "double")
  .create()

questionsWithSentimentsDF
  .select("id", "text", "votes", "datetime", "sentiment_score")
  .write
  .format(HIVE_WAREHOUSE_CONNECTOR)
  .mode("append")
  .option("table", "kritikarana_questions_with_sentiments")
  .save()

// Create "answers_with_sentiments" table
hive.createTable("kritikarana_answers_with_sentiments")
  .ifNotExists()
  .column("answer_id", "string")
  .column("q_id", "string")
  .column("text", "string")
  .column("votes", "bigint")
  .column("sentiment_score", "double")
  .create()

answersWithSentimentsDF
  .write
  .format(HIVE_WAREHOUSE_CONNECTOR)
  .mode("append")
  .option("table", "kritikarana_answers_with_sentiments")
  .save()
*/

questionsWithSentimentsDF.select("id", "text", "votes", "datetime", "sentiment_score")
    .write
    .format("parquet")
    .mode("append")
    .option("path", "wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/home/sshuser/kritikarana/project/hive/questions/")
    .saveAsTable("kritikarana_questions_with_sentiments")

answersWithSentimentsDF
    .write 
    .format("parquet") 
    .mode("append") 
    .option("path", "wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/home/sshuser/kritikarana/project/hive/answers/") 
    .saveAsTable("kritikarana_answers_with_sentiments")


println("Tables created and written successfully!")

