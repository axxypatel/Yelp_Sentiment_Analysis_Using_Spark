package batch

import akka.actor.Actor
import main_package.AppConfig
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.functions._
import util.{MLPreprocessing, SparkContextObject}

class MLModelTrain {

  //Create a Spark session which connect to Cassandra
  val spark = SparkContextObject.spark
  //Implicit methods available in Scala for converting common Scala objects into DataFrames

  //Get Spark Context from Spark session
  val sparkContext = spark.sparkContext

  def mlRetraining: Unit ={
    println("Model Retraining Started")
    //Read yelp_sentiment table using DataFrame
    val reviewDF = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "yelp_sentiment", "keyspace" -> "yelp_data"))
      .load()

    val reviewDFFinal = new MLPreprocessing(spark,reviewDF, true).reviewPreprocess()

    // Split the data into training and test sets (30% held out for testing)
    val Array(trainingData, testData) = reviewDFFinal.randomSplit(Array(0.7, 0.3), seed = 1234L)

    // Train a NaiveBayes model. - Slightly better accuracy achieved than Logistic Regression
    val model = new NaiveBayes().setModelType("multinomial").setLabelCol("sentiment").setFeaturesCol("features").fit(trainingData)

    // Select rows to transform.
    val predictions = model.transform(testData)
    predictions.show()

    // Evaluate Results
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("sentiment")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)
    println(s"NB Test set accuracy = $accuracy") // Accuracy of the model is 76%

    model.write.overwrite().save(AppConfig.modelPath)
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
