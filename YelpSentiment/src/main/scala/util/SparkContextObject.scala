package util

import org.apache.spark.sql.SparkSession

object SparkContextObject {
  val appName = "StructuredYelpSentiment"
  val master = "local[*]"

  lazy val spark = SparkSession.builder()
    .appName(appName)
    .master(master)
    .config("spark.neo4j.bolt.url","bolt://localhost:7687")
    .config("spark.neo4j.bolt.user","neo4j")
    .config("spark.neo4j.bolt.password","temp123")
    .config("spark.cassandra.connection.host", "localhost")
    .getOrCreate()

  def stopSparkSession() = spark.stop()

}
