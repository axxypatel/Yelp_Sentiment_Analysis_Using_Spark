package util
import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.util._
import org.apache.spark.ml.feature.NGram
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.log4j.Level
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.functions.countDistinct


object testJob {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("YelpSentiment")
    .getOrCreate()

  def testRun:Unit={
    val readBooksDF = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "yelp_sentiment", "keyspace" -> "yelp_data"))
      .load


    readBooksDF.printSchema
    readBooksDF.explain
    readBooksDF.show
  }

//  org.apache.log4j.Logger.getLogger("org").setLevel(Level.ERROR)
//
//  import spark.implicits._
//  //read data
//  //val path = "src/main/resources"
//  val reviewDF_all = spark.read.json("C:\\Users\\kahma\\Documents\\yelp_academic_dataset_review.json");
//  reviewDF_all.printSchema()
//  //reviewDF_all.groupBy("stars").count().orderBy("stars").show()
//
//  val reviewDF = reviewDF_all.limit(10000)
//  reviewDF.printSchema()
//  // separating our features and target
//  //val X = reviewDF.select("review_id","text")
//  //val y = reviewDF.select("review_id","stars")
//
//  val reviewDFwithout3 = reviewDF.filter(reviewDF("stars") === "1.0" || reviewDF("stars") === "2.0"
//    || reviewDF("stars") === "4.0" || reviewDF("stars") === "5.0")
//
//  val reviewDF_sentiment = reviewDFwithout3.withColumn("sentiment",
//    when(col("stars") === "4.0" || col("stars") === "5.0", 1)
//      .when(col("stars") === "1.0" || col("stars") === "2.0", 0))
//
//  reviewDF_sentiment.show(20)
//
//  // Remove punctuation and transfer to lowercase
//  reviewDF_sentiment.select("text").map(_.toString().replaceAll(
//    "[,.!?:;]", "")
//    .trim
//    .toLowerCase)
//
//  reviewDF_sentiment.printSchema()
//
//  //  val props = new Properties()
//  //  props.setProperty("annotators", "lemma")
//  //  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)
//  //
//  //  val reviewDF_sentiment = reviewDF.withColumn("lemma", pipeline.annotate(col("stars"))
//  //split words
//  val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
//  val tokenized = tokenizer.transform(reviewDF_sentiment)
//
//  tokenized.printSchema()
//
//  // remove stopwords
//  val remover = new StopWordsRemover()
//    .setInputCol("words")
//    .setOutputCol("filtered")
//
//  val stopwords_removed = remover.transform(tokenized)
//
//  stopwords_removed.printSchema()
//
//  val ngram = new NGram().setN(2).setInputCol("filtered").setOutputCol("ngrams")
//  val ngramDataFrame = ngram.transform(stopwords_removed)
//
//  ngramDataFrame.printSchema()
//
//  val hashingTF = new HashingTF()
//    .setInputCol("ngrams").setOutputCol("rawFeatures").setNumFeatures(20)
//
//  val featurizedData = hashingTF.transform(ngramDataFrame)
//  // alternatively, CountVectorizer can also be used to get term frequency vectors
//  featurizedData.printSchema()
//  val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
//  val idfModel = idf.fit(featurizedData)
//
//  val rescaledData = idfModel.transform(featurizedData)
//  rescaledData.printSchema()
//  // Split the data into training and test sets (30% held out for testing)
//  //val Array(trainingData, testData) = rescaledData.randomSplit(Array(0.7, 0.3), seed = 1234L)
//
//  //  // Train a NaiveBayes model.
//  val model = new NaiveBayes().setModelType("multinomial").setLabelCol("sentiment").setFeaturesCol("features").fit(rescaledData)
//
//  model.write.overwrite().save("C:\\Users\\kahma\\Documents\\MLModels")
//
//  print("Model saving is done")

}
