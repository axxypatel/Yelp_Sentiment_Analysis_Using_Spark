package data

import java.util.{Date, Properties}

import main_package.AppConfig
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

//Define Review class
case class Review(review_id:String,
                  user_id:String,
                  business_id: String,
                  user_loc: String, stars: Long,
                  date: Date,
                  text:String,
                  useful: Long,
                  funny: Long,
                  cool: Long
                )

object kafkaStreams {

  def run(): Unit = {
    //The Kafka Topic
    val kafkaTopic = AppConfig.kafkaTopic

    //Define a Kafka Producer
    val producer = new KafkaProducer[String, String](getKafkaProp)
    getReviews(producer,kafkaTopic)
  }

  def getReviews(producer: Producer[String, String],kafkaTopic:String): Unit = {
    //read from JSON

    val message = "keshav"
    //Send data to a Kafka topic
    val data = new ProducerRecord[String, String](kafkaTopic, message)
    producer.send(data)
  }


  //Define kafka properties
  def getKafkaProp():Properties={

    // create instance for properties to access producer configs
    val props = new Properties()
    //Assign localhost id
    props.put("bootstrap.servers", "localhost:9092")
    //Set acknowledgements for producer requests.
    props.put("acks", "all")
    //If the request fails, the producer can automatically retry,
    //props.put("retries", 0)
    //Specify buffer size in config
    //props.put("batch.size", 16384)
    //Reduce the no of requests less than 0
   // props.put("linger.ms", 1)
    //The buffer.memory controls the total amount of memory available to the producer for buffering.
    //props.put("buffer.memory", 33554432)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    return props
  }
}
