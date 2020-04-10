package batch

import akka.actor.Actor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}
import speed.StartProcessing
import util.SparkContextObject

object UserDataLoad {
  val spark = SparkContextObject.spark // changes done by akshay
  // Enable implicits like $
  import spark.implicits._

  // User schema for json data
  val userSchema = new StructType()
    .add("user_id", StringType, true)
    .add("name", StringType, true)
    .add("review_count", StringType, true)
    .add("yelping_since", StringType, true)
    .add("friends", StringType, true)
    .add("useful", StringType, true)
    .add("funny", StringType, true)
    .add("cool", StringType, true)
    .add("fans", StringType, true)
    .add("elite", StringType, true)
    .add("average_stars", StringType, true)
    .add("compliment_hot", StringType, true)
    .add("compliment_more", StringType, true)
    .add("compliment_profile", StringType, true)
    .add("compliment_cute", StringType, true)
    .add("compliment_list", StringType, true)
    .add("compliment_note", StringType, true)
    .add("compliment_plain", StringType, true)
    .add("compliment_cool", StringType, true)
    .add("compliment_funny", StringType, true)
    .add("compliment_writer", StringType, true)
    .add("compliment_photos", StringType, true)

  def loadData(filepath:String):Unit = {
    // Read user json file
    val userDf = spark.read.schema(userSchema).json(filepath)
    val finaluserDf = userDf.select(col("user_id"), col("name"), col("review_count"), col("yelping_since"), col("friends"),
      col("average_stars"))

    // Write data to casssandra table
    finaluserDf.write.format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "user", "keyspace" -> "yelp_data"))
      .save()

    print("User data loading is completed.")

  }

}

