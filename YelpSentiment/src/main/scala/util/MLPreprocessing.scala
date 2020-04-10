package util

import org.apache.spark.ml.feature._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.streaming.StreamingQuery

class MLPreprocessing(spark:SparkSession,reviewDF:DataFrame) {

  //Implicit methods available in Scala for converting common Scala objects into DataFrames
  import spark.implicits._
  //filter this is Kafka instead?
  def reviewPreprocess():DataFrame = {
          val reviewDFStarFilter = reviewDF.filter (reviewDF ("stars") === "1.0" || reviewDF ("stars") === "2.0"
          || reviewDF ("stars") === "4.0" || reviewDF ("stars") === "5.0")

          val reviewDFSentiment = reviewDFStarFilter.withColumn ("sentiment",
          when (col ("stars") === "4.0" || col ("stars") === "5.0", 1)
          .when (col ("stars") === "1.0" || col ("stars") === "2.0", 0) )

          // Remove punctuation and transfer to lowercase
          reviewDFSentiment.select ("text").map (_.toString ().replaceAll (
          "[,.!?:;]", "")
          .trim
          .toLowerCase)

          reviewDFSentiment.printSchema ()

          //split words
          val tokenizer = new Tokenizer ().setInputCol ("text").setOutputCol ("words")
          val reviewDFTokenized = tokenizer.transform (reviewDFSentiment)

          reviewDFTokenized.printSchema ()

          // remove stopwords
          val remover = new StopWordsRemover ()
          .setInputCol ("words")
          .setOutputCol ("filtered")

          val reviewDFNoStopwords = remover.transform (reviewDFTokenized)

          reviewDFNoStopwords.printSchema ()

          //ngrams - consider unigrams and bigrams
          val ngram = new NGram ().setN (2).setInputCol ("filtered").setOutputCol ("ngrams")
          val reviewDFNGrams = ngram.transform (reviewDFNoStopwords)

          reviewDFNGrams.printSchema ()

          // TF-IDF vectorizer
          val hashingTF = new HashingTF ()
          .setInputCol ("ngrams").setOutputCol ("features").setNumFeatures (20)

          val reviewDFHashed = hashingTF.transform (reviewDFNGrams)
          // alternatively, CountVectorizer can also be used to get term frequency vectors
          reviewDFHashed.printSchema ()
          return reviewDFHashed

          //reviewDFFinal.printSchema()

  }


}
