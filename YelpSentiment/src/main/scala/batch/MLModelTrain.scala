package batch

import akka.actor.Actor
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.sql.functions._
import util.{MLPreprocessing, SparkContextObject}
class MLModelTrain {

  //Create a Spark session which connect to Cassandra
  val spark = SparkContextObject.spark
  //Implicit methods available in Scala for converting common Scala objects into DataFrames

  //Get Spark Context from Spark session
  val sparkContext = spark.sparkContext

  def mlRetraining: Unit ={

    //Read yelp_sentiment table using DataFrame
    val reviewDF = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "yelp_review", "keyspace" -> "yelp_data"))
      .load()
      .filter(datediff(current_date(),col("timestamp")) > 0)

    val reviewDFFinal = new MLPreprocessing(spark,reviewDF).reviewPreprocess()
    // Split the data into training and test sets (30% held out for testing)
    //val Array(trainingData, testData) = reviewDFFinal.randomSplit(Array(0.7, 0.3), seed = 1234L)

    // Train a NaiveBayes model. - Slightly better accuracy achieved than Logistic Regression
    val model = new NaiveBayes().setModelType("multinomial").setLabelCol("sentiment").setFeaturesCol("features").fit(reviewDFFinal)

    // Now we can optionally save the fitted pipeline to disk
    model.write.overwrite().save("C:\\Users\\kahma\\Documents\\MLModels")
  }

  def reviewFileLoad:Unit = {

  }
}

case object MLBatchRetraining

//Define BatchProcessing actor

class MLBatch (spark_processor: MLModelTrain) extends Actor{

  //Implement receive method
  def receive = {
    //Start review sentiment batch processing
    case MLBatchRetraining => {
      println("\nStart ML Model training in batch processing...")
      // Perform batch processing
      spark_processor.mlRetraining
    }
  }

}