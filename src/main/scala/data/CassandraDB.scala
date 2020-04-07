package data

import com.datastax.driver.core.Cluster

object CassandraDB {

    def runDB(): Unit = {
      //creating Cluster object
      val cluster = Cluster.builder.addContactPoint("localhost").build
      //Creating Session object
      var session = cluster.connect
      var query=""

      query = "DROP KEYSPACE IF EXISTS yelp_data;"

      session.execute(query)

      //Query to create yelp_data keyspace
      // Using 'replication_factor':1 if only run on local machine
      query = "CREATE KEYSPACE IF NOT EXISTS yelp_data WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};"
      session.execute(query)

      //Connect to the lambda_architecture keyspace
      session = cluster.connect("yelp_data")

      query = "CREATE TABLE IF NOT EXISTS yelp_sentiment(review_id text PRIMARY KEY, user_id text, business_id text, user_loc text, review text, useful bigint, sentiment int);"
      session.execute(query)

      query = "CREATE TABLE IF NOT EXISTS review_verdict(business_id text PRIMARY KEY, count bigint);"
      session.execute(query)

      //Stop the connection
      println("Keyspace and tables were created successfully.")
      cluster.close()
    }
  }