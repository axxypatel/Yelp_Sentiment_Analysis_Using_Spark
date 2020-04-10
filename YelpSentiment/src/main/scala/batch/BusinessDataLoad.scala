package batch

import akka.actor.Actor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType,DecimalType,IntegerType}
import speed.StartProcessing
import util.SparkContextObject

object BusinessDataLoad {

  val spark = SparkContextObject.spark // changes done by akshay

  // User schema for json data
  val businessSchema = new StructType()
    .add("business_id", StringType, true)
    .add("name", StringType, true)
    .add("address", StringType, true)
    .add("city", StringType, true)
    .add("state", StringType,true)
    .add("postal_code",StringType , true)
    .add("latitude", StringType, true)
    .add("longitude", StringType, true)
    .add("stars", StringType, true)
    .add("review_count", StringType,true)
    .add("is_open", StringType, true)
    .add("attributes", StringType, true)
    .add("categories", StringType, true)
    .add("hours", StringType, true)

  def loadData(filepath:String):Unit = {
    // Read Json file and load into dataframe
    val businessDf = spark.read.schema(businessSchema).json(filepath)

    val finalbusinessDf = businessDf.select(col("business_id"), col("name"), col("address"),
      col("city"), col("state"), col("postal_code"),
      col("stars"), col("categories"))

    // Write data into cassandra table
    finalbusinessDf.write.format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "business", "keyspace" -> "yelp_data"))
      .save()
     print("Business data loading is completed.")
  }
}
