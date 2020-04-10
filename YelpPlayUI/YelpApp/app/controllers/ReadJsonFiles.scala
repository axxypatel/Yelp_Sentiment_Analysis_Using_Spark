package controllers
import javax.inject._
import play.api.mvc._

import scala.io._
import java.util.Properties

import com.datastax.driver.core._
import org.apache.kafka.clients.producer._
import org.neo4j.driver.{AuthTokens, GraphDatabase}

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

@Singleton
class ReadJsonFiles  @Inject()(cc: ControllerComponents) extends AbstractController(cc){

    def index = Action {
        Ok(views.html.homepage(Nil))
    }

    def readJson = Action {
        val topic1 = "ReviewTopic"
        val topic2 = "TipTopic"
        val reviewFilePath = "C://yelp_academic_dataset_review.json"
        val tipFilePath = "C://yelp_academic_dataset_tip.json"
        val cntList:List[Int] = List()

        // call function to read review json file data and return record Count
        //val reviewFileCnt:Int = jsonDataRead(topic1,reviewFilePath)
        //Ok(views.html.homepage(reviewFileCnt :: cntList))

        //Call Graph Function call
        val friends:List[String] = userGraphSearch
        //Ok(views.html.dashboard(friends))

        //Call Cassandra fucntion call
        //val friends:List[String] = connectDatabase
        Ok(views.html.dashboard(friends))

    }

    def jsonDataRead(topic:String,file:String): Int ={
        val props = new Properties()
        props.put("bootstrap.servers", "localhost:9092")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String, String](props)
        var cnt = 0
        val jsonFilePath = Source.fromFile(file)
        breakable {
            for (line <- jsonFilePath.getLines) {
                val record = new ProducerRecord[String, String](topic, line)
                producer.send(record)
                cnt += 1
               if (cnt == 100) break
            }
        }
        producer.close()
        return cnt
    }

    def userGraphSearch():List[String] ={

        val driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "temp123"))
        val session = driver.session
        val script = s"MATCH (u:User)-[:Knows]->(m:User) where u.user_id='CxDOIDnH8gp9KXzpBHJYXw' RETURN m.sec_node"
        val result = session.run(script)
        //var friendList = new ListBuffer[String]()
        var friendString = "("
        while (result.hasNext()) {
            var record = result.next()
            friendString += "'"+record.get("m.sec_node").asString()+ "'" + ","
        }
        friendString += ")"
        val finalString = friendString.replace(",)",")").replace(" ","")
        session.close()
        driver.close()
        connectDatabase(finalString)
        //return friendList.toList
    }

    def connectDatabase(friends:String):List[String]={
        val cluster = Cluster.builder().addContactPoint("localhost").build()
        val session = cluster.connect()
        var friendList = new ListBuffer[String]()
        //println(friends)
        val results = session.execute(s""" select * from yelp_data.user where user_id in $friends; """)
        while(results.iterator().hasNext()){
            var row = results.iterator().next()
            println(row.getString("name"))
            friendList+= row.getString("name")
        }
        session.close()
        cluster.close()
        return friendList.toList
    }
}
