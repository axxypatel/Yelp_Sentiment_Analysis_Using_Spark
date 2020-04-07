package main_package

import com.typesafe.config.ConfigFactory

import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration

object AppConfig {
  val config = ConfigFactory.load()

  val kafkaTopic=config.getString("kafka.topic")

  val reviewDuration=Duration.fromNanos(config.getDuration("batchProcessing.reviewDuration").toNanos)
  val batchInterval=Duration.fromNanos(config.getDuration("batchProcessing.batchInterval").toNanos)
}
