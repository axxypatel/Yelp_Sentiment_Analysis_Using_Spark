package main_package

import akka.actor.{ActorSystem, Props}
import batch._
import speed._

import scala.concurrent.duration._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.spark_project.dmg.pmml.True
import util.{CassandraDB, SetupEnv, SparkContextObject, testJob}

object yelpSentimentMain extends App{

  // Spark object
  val spark = SparkContextObject.spark

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  // Start Cassandra cluster
  println("Setting environment...")
  SetupEnv.start()

  // Connect to Cassandra database and create database and tables
  println("Creating database...")
  CassandraDB.runDB()

  // Load all user, business and userNetwork in database
 // jsonFileDataLoad()

  // Run Batch processing and realtime processing
  println("Run batch processing and realtime processing...")
  runProcessing()


  def runProcessing()={
    //Creating an ActorSystem
    val actorSystem = ActorSystem("ActorSystem");

    //Create realtime processing actor
    val mlActor = actorSystem.actorOf(Props(new MLSpeedLayer(new RealtimeProcessingSpark)))

    //Create batch actor for model retraining
    val MLTrainbatchActor = actorSystem.actorOf(Props(new MLBatch(new MLModelTrain)))

    //Create batch actor for model retraining
    val calculateAggScore = actorSystem.actorOf(Props(new BusinessScore(new CalculateScore)))

    //Using akka scheduler to run the batch processing periodically
    import actorSystem.dispatcher
    val initialDelay = 100 milliseconds

    actorSystem.scheduler.scheduleOnce(initialDelay,mlActor,StartProcessing)

   // actorSystem.scheduler.schedule(initialDelay,Duration.create(24*60*60,"seconds"),MLTrainbatchActor,MLBatchRetraining)
    actorSystem.scheduler.schedule(initialDelay,Duration.create(24*60*60,"seconds"),calculateAggScore,CalculateBusinessScore)

  }

  def jsonFileDataLoad():Unit={
    val user = UserDataLoad
    user.loadData("C:\\Users\\kahma\\Documents\\yelp_academic_dataset_user.json")

    val business = BusinessDataLoad
    business.loadData("C:\\Users\\kahma\\Documents\\yelp_academic_dataset_business.json")

    val checkin = CheckinDataLoad
    checkin.loadData("C:\\Users\\kahma\\Documents\\yelp_academic_dataset_checkin.json")

    val userGraph = UserNetworkLoad
    userGraph.prepareUserGraph()

  }
}
