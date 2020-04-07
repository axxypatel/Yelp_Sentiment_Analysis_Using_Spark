package main_package

import akka.actor.{ActorSystem, Props}
import batch.{BatchLayer, BatchProcessingSpark, ReviewSentimentProcessing}
import data.{CassandraDB, SetupEnv, kafkaStreams}
import speed.{SpeedLayer, RealtimeProcessingSpark}
import scala.concurrent.duration._

object yelpSentimentMain {
  def main(args: Array[String]): Unit = {
    // Start Zookeeper, Kafka server, Cassandra and Kafka Cassandra Connector
    println("Setting environment...")
    SetupEnv.start()

    // Connect to Cassandra database and create database
    println("Creating database...")
    CassandraDB.runDB()

    // Get Twitter streaming data and send to Kafka broker
    println("Getting data from kafka...")
    kafkaStreams.run()

    // Run Batch processing and realtime processing
    println("Run batch processing and realtime processing...")
    runProcessing()
  }
  def runProcessing()={
    //Creating an ActorSystem
    val actorSystem = ActorSystem("ActorSystem");

    //Create realtime processing actor
    val realtimeActor = actorSystem.actorOf(Props(new SpeedLayer(new RealtimeProcessingSpark)))

    //Create batch actor
    val batchActor = actorSystem.actorOf(Props(new BatchLayer(new BatchProcessingSpark,realtimeActor)))

    //Using akka scheduler to run the batch processing periodically
    import actorSystem.dispatcher
    val initialDelay = 100 milliseconds
    val batchInterval=AppConfig.batchInterval //running batch processing after each 30 mins

    actorSystem.scheduler.schedule(initialDelay,batchInterval,batchActor,ReviewSentimentProcessing)
  }
}
