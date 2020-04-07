package batch

import akka.actor.{Actor, ActorRef}
import org.apache.spark.sql.functions.desc
import com.datastax.spark.connector.cql.CassandraConnector
import main_package.AppConfig
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lower
import speed.StartProcessing
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

class BatchProcessingSpark{
  //Create a Spark session which connect to Cassandra
  val spark = org.apache.spark.sql.SparkSession
    .builder()
    .master("local[*]")
    .config("spark.cassandra.connection.host", "localhost")
    .appName("YelpSentimentAnalysis")
    .getOrCreate()

  //Implicit methods available in Scala for converting common Scala objects into DataFrames
  import spark.implicits._

  //Get Spark Context from Spark session
  val sparkContext = spark.sparkContext

  //Set the Log file level
  sparkContext.setLogLevel("WARN")

  def sentimentAnalysis: Unit ={

    //Read yelp_sentiment table using DataFrame
    val sentimentDF = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "yelp_sentiment", "keyspace" -> "yelp_data"))
      .load()

    val sentimentByBusinessDF = sentimentDF.groupBy("business_id").count()

    sentimentByBusinessDF.printSchema()

    //Connect Spark to Cassandra to remove all existing data from review_verdict table
    val connector = CassandraConnector(sparkContext.getConf)
    connector.withSessionDo(session => session.execute("TRUNCATE yelp_data.review_verdict"))

    //Save sentiment analysis to review_verdict table
    sentimentByBusinessDF.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace"->"yelp_data","table"->"review_verdict"))
      .mode(SaveMode.Append)
      .save()
  }
}

case object ReviewSentimentProcessing

//Define BatchProcessing actor

class BatchLayer (spark_processor: BatchProcessingSpark, realtimeActor:ActorRef) extends Actor{

  //Implement receive method
  def receive = {
    //Start review sentiment batch processing
    case ReviewSentimentProcessing => {
      println("\nStart review sentiment  batch processing...")

      // Send StartProcessing to realtimeActor to start/restart realtime analysis
      //realtimeActor!StartProcessing

      // Perform batch processing
      spark_processor.sentimentAnalysis
    }
  }
}
