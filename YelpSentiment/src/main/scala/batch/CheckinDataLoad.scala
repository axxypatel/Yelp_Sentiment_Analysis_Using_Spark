package batch

import akka.actor.Actor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}
import speed.StartProcessing
import util.SparkContextObject

object CheckinDataLoad {

  val spark = SparkContextObject.spark // changes done by akshay
  // Enable implicits like $
  import spark.implicits._

  // Checkin schema for json data
  val checkinSchema = new StructType()
    .add("business_id", StringType, true)
    .add("date", StringType, true)

  def loadData(filepath:String):Unit = {
    // Read json file and load into dataframe
    val checkinDf = spark.read.schema(checkinSchema).json(filepath)
    val finalCheckinDf = checkinDf.select (col ("business_id"), col ("date"))

    // Write data into cassandra
    finalCheckinDf.write.format("org.apache.spark.sql.cassandra")
      .mode("Append")
      .options(Map("table" -> "checkin", "keyspace" -> "yelp_data"))
      .save()
    print("Checkin data loading is completed.")
  }

}
