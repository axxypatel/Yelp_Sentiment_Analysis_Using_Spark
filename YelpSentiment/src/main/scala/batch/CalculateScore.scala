package batch

import akka.actor.Actor
import org.apache.log4j.Level
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import util.SparkContextObject
class CalculateScore {

  //Create a Spark session which connect to Cassandra
  val spark = SparkContextObject.spark
  //Implicit methods available in Scala for converting common Scala objects into DataFrames

  //Get Spark Context from Spark session
  val sparkContext = spark.sparkContext

  def CalScore: Unit = {
    println("(Re)Calculate Scores")
    //Read yelp_sentiment table using DataFrame
    val reviewDF = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "yelp_review", "keyspace" -> "yelp_data"))
      .load()

    org.apache.log4j.Logger.getLogger("org").setLevel(Level.ERROR)

    val joinedDF = createDFWithScore(spark, reviewDF)

    joinedDF.write.format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "yelp_score", "keyspace" -> "yelp_data"))
      .save()

  }

  def createDFWithScore(spark:SparkSession,reviewDF:DataFrame): DataFrame =
  {
    import spark.implicits._
    val windowByBusinessID = Window.partitionBy("business_id")
    val windowOrderByUseful = Window.partitionBy("business_id").orderBy(desc("useful"))

    val scoreDF = reviewDF.withColumn("row", row_number.over(windowOrderByUseful))
      .withColumn("score", avg(col("sentiment")).over(windowByBusinessID))
      .withColumn("reviewAggregate", sum(col("sentiment")).over(windowByBusinessID))
      .withColumn("totalCount", count(col("sentiment")).over(windowByBusinessID))
      .where(col("row") === 1).select("business_id", "score")

    scoreDF.show()
    val top5positiveDF = reviewDF.filter(col("sentiment") === 1.0).withColumn("rank_positive", rank().over(windowOrderByUseful)).where("rank_positive < 6")
    top5positiveDF.show()
    val top5negativeDF = reviewDF.filter(col("sentiment") === 0.0).withColumn("rank_negative", rank().over(windowOrderByUseful)).where("rank_negative < 6")
    top5negativeDF.show()


    val groupedByPositive = top5positiveDF.groupBy($"business_id")
      .agg(collect_set($"review")).alias("top5positive")

    val groupedByNegative = top5negativeDF.groupBy($"business_id")
      .agg(collect_set($"review")).alias("top5negative")

    val joinedDF = scoreDF.join(groupedByPositive, Seq("business_id"), "full_outer")
      .join(groupedByNegative, Seq("business_id"), "full_outer").orderBy(desc("score"))

    joinedDF

  }

  // df.where("city === Boston && category IN Italian").orderBy(desc("score")).take(100)
  //left outer join of above dataframe with checkin and then order by count(checkin dates) take(10)  }
}

case object CalculateBusinessScore

//Define BatchProcessing actor

class BusinessScore (spark_processor: CalculateScore) extends Actor{

  //Implement receive method
  def receive = {
    //Start review sentiment batch processing
    case CalculateBusinessScore => {
      println("\nGet sentiment analysis and performing aggregations...")
      // Perform batch processing
      spark_processor.CalScore
    }
  }

}
