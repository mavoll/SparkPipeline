package de.smartsquare.spark.batch

import akka.actor.{Actor, ActorSystem, Props}
import com.datastax.spark.connector._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scala.concurrent.duration.DurationInt
import akka.actor.actorRef2Scala

import de.smartsquare.spark.util.SparkPipelineConfigUtil._

class BatchProcessingUnit {

  val sparkConf = new SparkConf()
      .setAppName("Lambda_Batch_Processor").setMaster(sparkMaster) //"194.95.79.98[2]" 2 cores
      .set("spark.cassandra.connection.host", hosts(0))
      .set("spark.cassandra.auth.username", username)
      .set("spark.cassandra.auth.password", password)


  val sc = new SparkContext(sparkConf)

  def start: Unit ={

    /*val rddKeywords = sc.cassandraTable("batch_view", "tweets_filter_keywords")
      .select("table_name","type","keyword") //df.col("*")
      .where("table_name='tweets_hamburg_by_keyword'")
    
    val df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "tweets_hamburg", "keyspace" -> "master_dataset" ))
      .load()
      
   df.write
      .cassandraFormat("tweets_hamburg_filtered", "batch_view", "Test Cluster")
      .save()
    
    val rdd = sc.cassandraTable("master_dataset", "tweets_hamburg")
      .where("createdAt > ?", System.currentTimeMillis() - 62 * 1000)      
      .filter(row => row.getString("text").like("%Domplatz%") )   
      .saveToCassandra("batch_view","tweets_hamburg_filtered")
      
    
     .keyBy(row => (row.getInt("year"), row.getInt("month"), row.getInt("day")))
      .spanByKey
    val result = rdd.filter(row => row.getString("text").like("%Keywords%") )
      
     
      
    val count_total = result.count()   
    val count_hourly = result.count()  
    val count_monthly = result.count() 
    
    
    val collection = sc.parallelize(Seq(("tweets_counter_filtered", count_total)))
    collection.saveToCassandra("batch_view", "tweets_counter_filtered", SomeColumns("table_name", "tweets_counter"))
    
    val hourFormat = new SimpleDateFormat("HH")   
    val day = hourFormat.format(row.getLong("createdat")) 
    val collection2 = sc.parallelize(Seq(("tweets_counter_filtered", row.getInt("year"), row.getInt("month"), row.getInt("day"), day, count_hourly)))
    collection2.saveToCassandra("batch_view", "tweets_counter_hourly_filtered", SomeColumns("table_name", "year", "month", "day", "hour", "tweets_counter"))
        
    val collection3 = sc.parallelize(Seq(("tweets_counter_filtered", row.getInt("year"), row.getInt("month"), count_monthly)))
    collection3.saveToCassandra("batch_view", "tweets_counter_monthly_filtered", SomeColumns("table_name", "year", "month", "tweets_counter"))
    
   
    
    result.saveToCassandra("batch_view","tweets_hamburg_by_keyword")*/
        //47123read filterwords (pos and neg) from cassandra
    //filter tables
    //save to cassandra (add a Column "filteredByKeywords" and add map with (keyword, count))
    //count total hourly monthly decrease coutertables
    //(count number of mentions per filterwords)
    /*
    val rddKeywords = sc.cassandraTable("batch_view", "filter_keywords").select("type","keyword")
    
    val df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "tweets_hamburg", "keyspace" -> "master_dataset" ))
      .load()
      
   df.write
      .cassandraFormat("tweets_hamburg_filtered", "batch_view", "Test Cluster")
      .save()
    
    val rdd = sc.cassandraTable("master_dataset", "tweets_hamburg")
      .where("createdAt > ?", System.currentTimeMillis() - 62 * 1000)      
      .filter(row => row.getString("text").like("%Domplatz%") )   
      .saveToCassandra("batch_view","tweets_hamburg_filtered")
      
    
     .keyBy(row => (row.getInt("year"), row.getInt("month"), row.getInt("day")))
      .spanByKey
     val result = rdd.filter(row => row.getString("text").like("%Keywords%") )
     
     
     
    
    
     
      
    val count_total = result.count()   
    val count_hourly = result.count()  
    val count_monthly = result.count() 
    
    
    val collection = sc.parallelize(Seq(("tweets_counter_filtered", count_total)))
    collection.saveToCassandra("batch_view", "tweets_counter_filtered", SomeColumns("table_name", "tweets_counter"))
    
    val hourFormat = new SimpleDateFormat("HH")   
    val day = hourFormat.format(row.getLong("createdat")) 
    val collection2 = sc.parallelize(Seq(("tweets_counter_filtered", row.getInt("year"), row.getInt("month"), row.getInt("day"), day, count_hourly)))
    collection2.saveToCassandra("batch_view", "tweets_counter_hourly_filtered", SomeColumns("table_name", "year", "month", "day", "hour", "tweets_counter"))
        
    val collection3 = sc.parallelize(Seq(("tweets_counter_filtered", row.getInt("year"), row.getInt("month"), count_monthly)))
    collection3.saveToCassandra("batch_view", "tweets_counter_monthly_filtered", SomeColumns("table_name", "year", "month", "tweets_counter"))
    
   
    
    result.saveToCassandra("batch_view","tweets_hamburg_filtered")*/
   
  }
}

case object StartBatchProcess

class BatchProcessingActor(processor: BatchProcessingUnit) extends Actor  {

  implicit val dispatcher = context.dispatcher

  val initialDelay = 1000 milli
  val interval = 60 seconds


  context.system.scheduler.schedule(initialDelay, interval, self, StartBatchProcess)

  def receive: PartialFunction[Any, Unit] = {

    case StartBatchProcess => processor.start

  }

}

object BatchProcessor extends App {

  val actorSystem = ActorSystem("BatchActorSystem")

  val processor = actorSystem.actorOf(Props(new BatchProcessingActor(new BatchProcessingUnit)))

  processor ! StartBatchProcess

}
