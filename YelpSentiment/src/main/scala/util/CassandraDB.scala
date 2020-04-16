package util

import com.datastax.driver.core.Cluster

object CassandraDB {

    def runDB(): Unit = {
      //creating Cluster object
      val cluster = Cluster.builder.addContactPoint("localhost").build
      //Creating Session object
      var session = cluster.connect
      var query=""

      //query = "DROP KEYSPACE IF EXISTS yelp_data;"
      //session.execute(query)

      //Query to create yelp_data keyspace
      // Using 'replication_factor':1 if only run on local machine
      query = "CREATE KEYSPACE IF NOT EXISTS yelp_data WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};"
      session.execute(query)

      //Connect to the lambda_architecture keyspace
      session = cluster.connect("yelp_data")

      query = "CREATE TABLE IF NOT EXISTS yelp_data.yelp_sentiment(review_id text , user_id text, business_id text, user_loc text, review text, useful text, sentiment double, primary key(user_id,business_id));"
      session.execute(query)

      query = "CREATE TABLE IF NOT EXISTS yelp_data.user(user_id text PRIMARY KEY, name text, review_count text, yelping_since text, friends text, average_stars text);"
      session.execute(query)

      query = "CREATE TABLE IF NOT EXISTS yelp_data.business(business_id text PRIMARY KEY, name text, address text, city text, state text, postal_code text, stars text, categories text);"
      session.execute(query)

      query = "CREATE TABLE IF NOT EXISTS yelp_data.checkin(business_id text PRIMARY KEY, date text);"
      session.execute(query)

      //Stop the connection
      println("Keyspace and tables were created successfully.")
      cluster.close()
    }
  }
