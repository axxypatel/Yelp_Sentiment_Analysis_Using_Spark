package controllers
import javax.inject._
import play.api.mvc._

import scala.collection.JavaConverters._
import scala.io._
import java.util.Properties

import com.datastax.driver.core.{BoundStatement, Cluster, ResultSet, Row, Session}
import org.apache.kafka.clients.producer._
import org.neo4j.driver.{AuthTokens, GraphDatabase}

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
import views._

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.util.Try
case class Business(score: Double, top5positive: List[String], top5negative: List[String])

@Singleton
class homeController  @Inject()(cc: ControllerComponents) extends AbstractController(cc){

    def index = Action {
        Ok(views.html.home(Nil))
    }

    def time[R](block: => R): R = {
      val t0 = System.nanoTime() / 1000000000
      val result = block    // call-by-name
      val t1 = System.nanoTime()  / 1000000000
      println("Elapsed time: " + (t1 - t0)  + "sec")
      result
    }
    def initializeKafkaStream = Action {
        val topic1 = AppConfig.reviewTopic
        val reviewFilePath = AppConfig.reviewFile
        val cntList:List[Int] = List()

        import views._

        // call function to read review json file data and return record Count
        val reviewFileCnt:Int = time{sendKafkaMessages(topic1,reviewFilePath)}
        //val reviewFileCnt:Int = 100

        Ok(views.html.home(reviewFileCnt :: cntList))

    }

    def sendKafkaMessages(topic:String, file:String): Int ={
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
                //if (cnt == 100) break
            }
        }
        producer.close()
        cnt
    }

    def loadBusinessData(user_id:String, business_name:String) = Action {

        //Invoke Cassandra
        val cluster = Cluster.builder().addContactPoint("localhost").build()
        val session = cluster.connect

        val queryString:String =  "SELECT * FROM yelp_data.yelp_score " + "WHERE business_id = :business_id"

        val prepared = session.prepare(queryString)

         val bound:BoundStatement = prepared.bind().setString("business_id", business_name)

        val row = session.execute(bound).one()
//        while (rs.iterator().hasNext) {
//            val row = rs.iterator().next()
//            val business_score = row.getString("score")
//            val top5positive = row.getSet("top5positive",classOf[String]).asScala.toList
//            val top5negative = row.getSet("top5negative",classOf[String]).asScala.toList/        }

        val obj = Business(
            row.getDouble("score"),
            row.getSet("top5positive", classOf[String]).asScala.toList,
            row.getSet("top5negative", classOf[String]).asScala.toList)


       // val positive = (1 to top5positive).zip(top5positive).toMap
        //val negative = (1 to top5negative.size).zip(top5negative).toMap
        //val friends = (1 to top5positive.size).zip(top5positive).toMap //Attention: load the user friend's network here

      val friends = time{getUserGraph(user_id,business_name)}
      Ok(views.html.business(business_name,obj.score,obj.top5positive,obj.top5negative,friends))
    }


    def getFriendReviews(friends:String,business_name:String):List[String]={
        val cluster = Cluster.builder().addContactPoint("localhost").build()
        val session = cluster.connect()
        var friendList = new ListBuffer[String]()
        var temp = "'"+business_name+"'"
        //println(friends)
        val results = session.execute(s""" select review,user_id from yelp_data.yelp_sentiment where user_id in $friends and business_id = $temp allow filtering; """)
        while(results.iterator().hasNext()){
            var row = results.iterator().next()
            println(row.getString("review"))
            friendList+= row.getString("review")
        }
        session.close()
        cluster.close()
        friendList.toList
    }

    def getUserGraph(user_id:String,business_name:String):List[String] ={

        val driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "temp123"))
        val session = driver.session
        var temp = "'"+user_id+"'"
        val script = s"MATCH (u:User)-[:Knows]->(m:User) where u.user_id= $temp RETURN m.sec_node"
        val result = session.run(script)
        //var friendList = new ListBuffer[String]()
        var friendString = "("
        while (result.hasNext()) {
            var record = result.next()
            friendString += "'"+record.get("m.sec_node").asString()+ "'" + ","
        }
      //print(friendString)
        friendString += ")"
        val finalString = friendString.replace(",)",")").replace(" ","")
        session.close()
        driver.close()
        getFriendReviews(finalString,business_name)
    }

    def loadTrendingData(category:String, city:String) = Action{
        val cluster = Cluster.builder().addContactPoint("localhost").build()
        val session = cluster.connect()
        //val temp = "'"+city+"'"
        //val temp2 = "'" + category + "'"
        val businesses = session.execute(s""" select categories,business_id,name from yelp_data.business where city = $city allow filtering; """)
        val relevantBusinesses = scala.collection.mutable.ListMap("No Relevant Businesses Found" -> "0.0")
        while (businesses.iterator().hasNext) {
            val row = businesses.iterator().next()
            val categories = Try { row.getString("categories").toString }.getOrElse("no value")
            val business_id =  Try { row.getString("business_id").toString }.getOrElse("no value")
            val business_name = Try { row.getString("name").toString }.getOrElse("no value")
            println(business_id)

            val relevant = categories.contains(category.replaceAll("\'", ""))

            if (relevant) {
                val business = session.execute(s""" select score from yelp_data.yelp_score where business_id = '$business_id' """)
                //while (business.iterator().hasNext){
                val r = business.iterator().next()
                val score = Try { r.getDouble("score").toString}.getOrElse("0.0")
                //}
                relevantBusinesses += (business_name -> score)
            }

        }
        session.close()
        cluster.close()

        val business_map = mutable.ListMap(relevantBusinesses.toSeq.sortBy(_._2):_*)
        Ok(views.html.trendingBusiness(category,city,business_map.take(10)))
    }



}