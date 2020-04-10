package batch
import util._
import org.apache.spark.sql.functions.{col, current_date, datediff, explode, split}
import org.neo4j.spark.dataframe.Neo4jDataFrame

object UserNetworkLoad {

  val spark = SparkContextObject.spark
  import spark.implicits._

  def prepareUserGraph():Unit={

    // Read user data from cassandra
    val reviewDF = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "user", "keyspace" -> "yelp_data"))
      .load()
      .select("user_id","name","friends").filter($"user_id" isin("CxDOIDnH8gp9KXzpBHJYXw"))

    // Create edge dataframe
    val edgeDf = reviewDF.select(col("user_id"),col("name"),explode(split(col("friends"),",")).alias("sec_node"))

    // Create Neo4jDataFrame to load all the edges into graph database
    Neo4jDataFrame.mergeEdgeList(spark.sparkContext,edgeDf,("User",Seq("name","user_id")),("Knows",Seq.empty),("User",Seq("sec_node")))
    print("Graph Load is done")
  }

}
