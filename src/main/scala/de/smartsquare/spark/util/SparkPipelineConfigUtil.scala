package de.smartsquare.spark.util

import scala.collection.JavaConversions._

import com.typesafe.config.ConfigFactory


object SparkPipelineConfigUtil {

  val config = ConfigFactory.load()
  
  val consumerKey = config.getString("app.consumerKey")
  val consumerSecret = config.getString("app.consumerSecret")
  val accessToken = config.getString("app.accessToken")
  val accessTokenSecret = config.getString("app.accessTokenSecret")
  val twitterKeyWords = config.getStringList("app.twitterKeyWords").toList
  val boundingBoxNorthEastLon = config.getDouble("app.boundingBox_north_east_lon")
  val boundingBoxNorthEastLat = config.getDouble("app.boundingBox_north_east_lat")
  val boundingBox1SouthWestLon = config.getDouble("app.boundingBox1_south_west_lon")
  val boundingBox1SouthWestLat = config.getDouble("app.boundingBox1_south_west_lat")
  val clusterName = config.getString("cassandra.clustername")
  val username = config.getString("cassandra.username")
  val password = config.getString("cassandra.password")
  val port = config.getInt("cassandra.port")
  val hosts = config.getStringList("cassandra.hosts").toList
  val cassandraKeyspaces = config.getStringList("cassandra.keyspaces").toList
  val cassandraTables = config.getStringList("cassandra.tables").toList
  val cassandraCounterTables = config.getStringList("cassandra.counter_tables").toList
  val replicationFactor = config.getInt("cassandra.replication_factor")
  val readConsistency = config.getString("cassandra.read_consistency")
  val writeConsistency = config.getString("cassandra.write_consistency")
  val bootstrapServers = config.getString("kafka.bootstrap_servers")
  val zookeeperConnect = config.getString("kafka.zookeeper_connect")
  val brokerId = config.getString("kafka.broker_id")
  val groupId = config.getString("kafka.group_id")
  val kafkaTopic = config.getStringList("kafka.kafka_topic").toList
  val akkaHttpIp = config.getString("kafka.akka_http_server_ip")
  val akkaHttpPort = config.getInt("kafka.akka_http_server_port")
  val masterIp = config.getString("spark.master_ip")
  val sparkMaster = config.getString("spark.spark_master")
  
}