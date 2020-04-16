package controllers.AppConfig

import com.typesafe.config.ConfigFactory

object AppConfig {
  val config = ConfigFactory.load()

  val reviewFile = config.getString("yelpdata.reviewFile")

  val reviewTopic = config.getString("yelpdata.reviewTopic")
}