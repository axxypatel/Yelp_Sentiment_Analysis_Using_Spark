package main_package

import java.text.SimpleDateFormat
import java.util.Calendar

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
  jsonFileDataLoad()

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
    val initialSpeedDelay = 100 milliseconds
    val now = Calendar.getInstance().getTime()
    val hourFormat = new SimpleDateFormat("hh")
    val currentHour = hourFormat.format(now)
    val initialBatchDelay = 24 - currentHour.toInt + 6

    println(initialSpeedDelay)
    println("initialBatchDelay")
    println(initialBatchDelay)
    actorSystem.scheduler.scheduleOnce(initialSpeedDelay,mlActor,StartProcessing)
    actorSystem.scheduler.schedule(Duration.create(initialBatchDelay,"hours"),Duration.create(24*60*60,"seconds"),MLTrainbatchActor,MLBatchRetraining)
    actorSystem.scheduler.schedule(Duration.create(30,"minutes"),Duration.create(30,"minutes"),calculateAggScore,CalculateBusinessScore)

  }

  def jsonFileDataLoad():Unit={
    val user = UserDataLoad
    user.loadData(AppConfig.userFile)

    val business = BusinessDataLoad
    business.loadData(AppConfig.businessFile)

    val checkin = CheckinDataLoad
    checkin.loadData(AppConfig.checkinFile)

    val userGraph = UserNetworkLoad
    userGraph.prepareUserGraph()
  }
}
