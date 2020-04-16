package main_package

import com.typesafe.config.ConfigFactory

import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration

object AppConfig {
  val config = ConfigFactory.load()

  val modelPath = config.getString("model.modelPath")

  val zooKeeperPath = config.getString("setupenv.zooKeeperPath")
  val serverPath = config.getString("setupenv.serverPath")
  val cassandraPath = config.getString("setupenv.cassandraPath")

  val businessFile = config.getString("yelpdata.businessFile")
  val userFile = config.getString("yelpdata.userFile")
  val checkinFile = config.getString("yelpdata.checkinFile")
}
