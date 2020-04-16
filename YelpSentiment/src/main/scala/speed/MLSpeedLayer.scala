package speed

import akka.actor.Actor
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.types.{DateType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{desc, from_json, lower}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF}
import org.apache.spark.sql.DataFrame
import util.{MLPreprocessing, SparkContextObject}


class RealtimeProcessingSpark{
  val spark = SparkContextObject.spark

  //Set the Log file level
  spark.sparkContext.setLogLevel("WARN")

  //Implicit methods available in Scala for converting common Scala objects into DataFrames
  import spark.implicits._

  //Define Schema of received tweet streem
  val reviewDataScheme
  = StructType(
    List(
      StructField("review_id", StringType, true),
      StructField("user_id", StringType, true),
      StructField("business_id", StringType, true),
      StructField("user_loc", StringType, true),
      StructField("stars", StringType, true),
      StructField("date", StringType, true),
      StructField("text", StringType, true),
      StructField("useful", StringType, true),
      StructField("funny", StringType, true),
      StructField("cool", StringType, true)
    )
  )

  def realtimeAnalysis(): Unit = {

    //Subscribe Spark the defined Kafka topic
    val reviewDFRead = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "ReviewTopic")
      .load()
      .selectExpr("CAST(value as STRING)")

    val reviewDF = reviewDFRead.select(from_json($"value", reviewDataScheme).alias("data1")).select("data1.*")
    val reviewPreprocessedDF = new MLPreprocessing(spark,reviewDF, false).reviewPreprocess()
    print("ML Preprocessing is done")



    val model = NaiveBayesModel.load("C:\\Users\\kahma\\Documents\\MLModels")

    val toBeStoredinCassandra = model.transform(reviewPreprocessedDF)
    toBeStoredinCassandra.printSchema()

    val writeDF =toBeStoredinCassandra.select(col("review_id"),
      col("user_id"),
      col("business_id"),
      col("user_loc"),
      col("text").as("review"),
      col(colName = "useful"),
      col("sentiment"))

    val query = writeDF.writeStream.foreachBatch{(batchDF, batchId) =>
      batchDF.write.format("org.apache.spark.sql.cassandra")
        .mode("Append")
        .options(Map("table" -> "yelp_sentiment", "keyspace" -> "yelp_data"))
        .save()
    }.outputMode("append").start()

    query.awaitTermination()
  }
}

case object StartProcessing

class MLSpeedLayer (spark_realtimeProc: RealtimeProcessingSpark) extends Actor{

  //Implement receive method
  def receive = {
    //Start realtime processing
    case StartProcessing => {
      println("\nStart realtime processing...")
      spark_realtimeProc.realtimeAnalysis()
    }
  }
}