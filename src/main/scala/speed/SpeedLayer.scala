package speed

import akka.actor.Actor
import com.datastax.spark.connector.cql.CassandraConnector
import main_package.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{desc, from_json, lower}
import org.apache.spark.sql.streaming.StreamingQuery
import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{NaiveBayes}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.NGram

class RealtimeProcessingSpark{
  //Define a Spark session
  val spark=SparkSession.builder().appName("yelp speed layer")
    .master("local")
    .getOrCreate()

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
      StructField("stars", LongType, true),
      StructField("date", DateType, true),
      StructField("text", StringType, true),
      StructField("useful", LongType, true),
      StructField("funny", LongType, true),
      StructField("cool", LongType, true)
    )
  )

  def realtimeAnalysis(): Unit = {
    //Subscribe Spark the defined Kafka topic
    val reviewDF = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", AppConfig.kafkaTopic)
      .load()
    reviewDF.printSchema()

    //filter this is Kafka instead?
    val reviewDFStarFilter = reviewDF.filter(reviewDF("stars") === "1.0" || reviewDF("stars") === "2.0"
      || reviewDF("stars") === "4.0" || reviewDF("stars") === "5.0")

    val reviewDFSentiment = reviewDFStarFilter.withColumn("sentiment",
      when(col("stars") === "4.0" || col("stars") === "5.0", 1)
        .when(col("stars") === "1.0" || col("stars") === "2.0", 0))

    // Remove punctuation and transfer to lowercase
    reviewDFSentiment.select("text").map(_.toString().replaceAll(
      "[,.!?:;]", "")
      .trim
      .toLowerCase)

    reviewDFSentiment.printSchema()

    //split words
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val reviewDFTokenized = tokenizer.transform(reviewDFSentiment)

    reviewDFTokenized.printSchema()

    // remove stopwords
    val remover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("filtered")

    val reviewDFNoStopwords = remover.transform(reviewDFTokenized)

    reviewDFNoStopwords.printSchema()

    //ngrams - consider unigrams and bigrams
    val ngram = new NGram().setN(2).setInputCol("filtered").setOutputCol("ngrams")
    val reviewDFNGrams = ngram.transform(reviewDFNoStopwords)

    reviewDFNGrams.printSchema()

    // TF-IDF vectorizer
    val hashingTF = new HashingTF()
      .setInputCol("ngrams").setOutputCol("rawFeatures").setNumFeatures(20)

    val reviewDFHashed = hashingTF.transform(reviewDFNGrams)
    // alternatively, CountVectorizer can also be used to get term frequency vectors
    reviewDFHashed.printSchema()

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(reviewDFHashed)

    val reviewDFFinal = idfModel.transform(reviewDFHashed)
    reviewDFFinal.printSchema()

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
    println(s"NB Test set accuracy = $accuracy")

    val writeDF =predictions.select(col("review_id"),
      col("user_id"),
      col("business_id"),
      col("user_loc"),
      col("text").as("review"),
      col(colName = "useful"),

      col("sentiment"))

    //Saving data to Cassandra
    writeDF.writeStream.outputMode("complete")
     .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "yelp_sentiment", "keyspace" -> "yelp_data"))
      .start()
  }
}

case object StartProcessing

class SpeedLayer (spark_realtimeProc: RealtimeProcessingSpark) extends Actor{

  //Implement receive method
  def receive = {
    //Start realtime processing
    case StartProcessing => {
      println("\nStart realtime processing...")
      spark_realtimeProc.realtimeAnalysis()
    }
  }
}