name := "YelpApp"
 
version := "1.0"

      
lazy val `test` = (project in file(".")).enablePlugins(PlayScala)
resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases"
      
resolvers += "Akka Snapshot Repository" at "https://repo.akka.io/snapshots/"
      
scalaVersion := "2.12.2"

libraryDependencies ++= Seq( jdbc , ehcache , ws , specs2 % Test , guice )

// https://mvnrepository.com/artifact/org.apache.kafka/kafka
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.4.1"

libraryDependencies += "org.neo4j.driver" % "neo4j-java-driver" % "4.0.1"

// https://mvnrepository.com/artifact/com.datastax.cassandra/cassandra-driver-mapping
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-mapping" % "3.8.0"

//unmanagedResourceDirectories in Test <+=  baseDirectory ( _ /"target/web/public/test" )

      