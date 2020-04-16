package util

case object StartZookeeper
case object StartKafkaServer
case object StartCassandra
case object StartTopic

class SetupActor extends Actor{
  //val kafkaDir="C:\\kafka"
  //val cassandraDir="C:\\cassandra\\apache-cassandra-3.11.6"
  val zookeeperCmd=AppConfig.zooKeeperPath
  val serverCmd=AppConfig.serverPath
  //val reviewTopic = ".\\bin\\windows\\kafka-console-producer.bat --broker-list localhost:9092 --topic TipTopic"
  val cassandraCmd= AppConfig.cassandraPath
  //val kafkaCassandraCmd="bin/connect-standalone.sh config/connect-standalone.properties config/cassandra-sink.properties"

  //Implement cmdExecuter method to execute a command string
  def cmdExecuter(cmd: String)={
    //using .exec() method to run the command
    val process = Runtime.getRuntime().exec(cmd)

    //Print the output of the process
    val reader = new BufferedReader(new InputStreamReader(process.getInputStream))
    var line:String= reader.readLine()
    while (line != null) {
      System.out.println(line)
      line = reader.readLine()
    }
    //Waiting until the process has been terminated
    process.waitFor()
  }

  //Implement receive method
  def receive = {

    //Start Zookeeper
    case StartZookeeper => {
      println("\nStart Zookeeper...")
     cmdExecuter(zookeeperCmd)
    }

    //Start Kafka Server
    case StartKafkaServer => {
      println("\nStart Kafka Server...")
      cmdExecuter(serverCmd)
    }

    //Start Cassandra
    case StartCassandra => {
      println("\nStart Cassandra ...")
      cmdExecuter(cassandraCmd)
    }

  }
}

object SetupEnv {
  //try{
    def start(): Unit = {
      {
        val actorSystem = ActorSystem("ActorSystem");
        val actor1 = actorSystem.actorOf(Props[SetupActor])
        val actor2 = actorSystem.actorOf(Props[SetupActor])
        val actor3 = actorSystem.actorOf(Props[SetupActor])
        val actor4 = actorSystem.actorOf(Props[SetupActor])

        //Send message to each actor to ask them doing their job
        //actor1 ! StartZookeeper
        //Waiting until the Zookeeper successfully started
        //Thread.sleep(10000)

        //actor2 ! StartKafkaServer
        //Thread.sleep(10000)

        actor3 ! StartCassandra
        Thread.sleep(5000)
      }
    }
  //}
}
