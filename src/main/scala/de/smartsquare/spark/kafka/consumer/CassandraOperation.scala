package de.smartsquare.spark.kafka.consumer

import java.util.Date
import java.text.SimpleDateFormat
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import de.smartsquare.spark.util.{Tweet, JsonHelper}
import de.smartsquare.spark.util.SparkPipelineConfigUtil._

case class TweetSummary(createdAt: Long, tweetId: Long, userName: String, lang: String, text: String, geoLocationLatitude: Double, geoLocationLongitude: Double)
case class ResponseTweets(numberOfTweets: Long, data: List[TweetSummary])

object CassandraOperation extends CassandraConnection  with JsonHelper{

  def insertTweets(listJson: List[String], batchSize: Int, tableName: String = cassandraTables.get(0)) = {
    println(":::::::::::::::::::::::::::: batch inserted")
    listJson.foreach{json => 
      cassandraConn.execute(s"INSERT INTO $tableName JSON '$json'")
      var jsontmp = parse(json)
      var m = extractTweetOrEmptyString(jsontmp)
      if(m != null){
        var year = m.year.getOrElse(0)
        var month = m.month.getOrElse(0)
        var day = m.day.getOrElse(0)
        var hour = m.hour.getOrElse(0)             
        cassandraConn.execute(s"UPDATE tweets_counter_hourly SET tweets_counter = tweets_counter + 1 WHERE table_name='$tableName' AND year=${year} AND month=${month} AND day=${day} AND hour=${hour}")
        cassandraConn.execute(s"UPDATE tweets_counter_monthly SET tweets_counter = tweets_counter + 1 WHERE table_name='$tableName' AND year=${year} AND month=${month}")
        cassandraConn.execute(s"UPDATE tweets_counter_daily SET tweets_counter = tweets_counter + 1 WHERE table_name='$tableName' AND year=${year} AND month=${month} AND day=${day}")
        cassandraConn.execute(s"UPDATE tweets_counter SET tweets_counter = tweets_counter + 1 WHERE table_name='$tableName'") 
        
        if(m.placeBoundingBoxCoordinates.isEmpty == false || (m.geoLocationLatitude.isEmpty == false && m.geoLocationLongitude.isEmpty == false )){
          val table_name_2 = cassandraTables.get(1)
          cassandraConn.execute(s"INSERT INTO $table_name_2 JSON '$json'")      
          cassandraConn.execute(s"UPDATE tweets_counter_hourly SET tweets_counter = tweets_counter + 1 WHERE table_name='$table_name_2' AND year=${year} AND month=${month} AND day=${day} AND hour=${hour}")
          cassandraConn.execute(s"UPDATE tweets_counter_monthly SET tweets_counter = tweets_counter + 1 WHERE table_name='$table_name_2' AND year=${year} AND month=${month}")
          cassandraConn.execute(s"UPDATE tweets_counter_daily SET tweets_counter = tweets_counter + 1 WHERE table_name='$table_name_2' AND year=${year} AND month=${month} AND day=${day}")
          cassandraConn.execute(s"UPDATE tweets_counter SET tweets_counter = tweets_counter + 1 WHERE table_name='$table_name_2'") 
        
        }
        if(m.lang.isEmpty == false ){
          val table_name_3 = cassandraTables.get(2)
          val lang = m.lang.getOrElse("") 
          cassandraConn.execute(s"INSERT INTO $table_name_3 JSON '$json'")      
          cassandraConn.execute(s"UPDATE tweets_counter_by_lang_hourly SET tweets_counter = tweets_counter + 1 WHERE table_name='$table_name_3' AND year=${year} AND month=${month} AND day=${day} AND hour=${hour} AND lang='$lang'")
          cassandraConn.execute(s"UPDATE tweets_counter_by_lang_monthly SET tweets_counter = tweets_counter + 1 WHERE table_name='$table_name_3' AND year=${year} AND month=${month} AND lang='$lang'")
          cassandraConn.execute(s"UPDATE tweets_counter_by_lang_daily SET tweets_counter = tweets_counter + 1 WHERE table_name='$table_name_3' AND year=${year} AND month=${month} AND day=${day} AND lang='$lang'")
          cassandraConn.execute(s"UPDATE tweets_counter_by_lang SET tweets_counter = tweets_counter + 1 WHERE table_name='$table_name_3' AND lang='$lang'") 
                   
        }
        if(m.userId.isEmpty == false ){
          val table_name_4 = cassandraTables.get(3)
          val userLang = m.userLang.getOrElse("")
          val userName = m.userName.getOrElse("")
          val userId = m.userId.getOrElse(0)
          cassandraConn.execute(s"INSERT INTO $table_name_4 JSON '$json'")      
          cassandraConn.execute(s"UPDATE tweets_counter_by_user_hourly SET tweets_counter = tweets_counter + 1 WHERE table_name='$table_name_4' AND year=${year} AND month=${month} AND day=${day} AND hour=${hour} AND userLang='$userLang' AND userName='$userName' AND userId=${userId}")
          cassandraConn.execute(s"UPDATE tweets_counter_by_user_daily SET tweets_counter = tweets_counter + 1 WHERE table_name='$table_name_4' AND year=${year} AND month=${month} AND day=${day} AND userLang='$userLang' AND userName='$userName' AND userId=${userId}")
          cassandraConn.execute(s"UPDATE tweets_counter_by_user_monthly SET tweets_counter = tweets_counter + 1 WHERE table_name='$table_name_4' AND year=${year} AND month=${month} AND userLang='$userLang' AND userName='$userName' AND userId=${userId}")
           
        }
      }
    }    
  }
  def getTweets(from_hour: String, to_hour: String, upperleft_lat: String, upperleft_lon: String, lowerright_lat: String, lowerright_lon: String, lang: String, userId: String, city: String): ResponseTweets = {
    
    val format = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss")
    var timestamp_from = format.parse(from_hour).getTime()
    var timestamp_to = format.parse(to_hour).getTime()   
    
    var select: String = ""
    // could be more efficiant by considering cassandra partitioning key
    city match {
      case "Hamburg" =>         
        if(lang != "None"){
          select = "select createdAt, tweetId, userName, lang, text, geoLocationLatitude, geoLocationLongitude from master_dataset.tweets_hamburg_by_lang where lang = " + lang + " and createdAt >= " + timestamp_from + " and createdAt <= " + timestamp_to + " ALLOW FILTERING;"; 
        }
        else if (userId != "None"){
          select = "select createdAt, tweetId, userName, lang, text, geoLocationLatitude, geoLocationLongitude from master_dataset.tweets_hamburg_by_user where userId = " + userId + " and createdAt >= " + timestamp_from + " and createdAt <= " + timestamp_to + " ALLOW FILTERING;";
        }
        else if (upperleft_lat != "None" && upperleft_lon != "None" && lowerright_lat != "None" && lowerright_lon != "None"){
          select = "select createdAt, tweetId, userName, lang, text, geoLocationLatitude, geoLocationLongitude from master_dataset.tweets_hamburg_located where geoLocationLatitude >= " + upperleft_lat + " and geoLocationLatitude <= " + lowerright_lat + " and geoLocationLongitude < " + lowerright_lon + " and geoLocationLongitude >= " + upperleft_lon + " and createdAt >= " + timestamp_from + " and createdAt <= " + timestamp_to + " ALLOW FILTERING;";
        }
        else{
          select = "select createdAt, tweetId, userName, lang, text, geoLocationLatitude, geoLocationLongitude from master_dataset.tweets_hamburg where createdAt >= " + timestamp_from + " and createdAt <= " + timestamp_to + " ALLOW FILTERING;";
        }
      case "Berlin" => 
        if(lang != "None"){
          select = "select createdAt, tweetId, userName, lang, text, geoLocationLatitude, geoLocationLongitude from master_dataset.tweets_berlin_by_lang where lang = " + lang + " and createdAt <= " + timestamp_to + " ALLOW FILTERING;";  
        }
        else if (userId != "None"){
          select = "select createdAt, tweetId, userName, lang, text, geoLocationLatitude, geoLocationLongitude from master_dataset.tweets_berlin_by_user where userId = " + userId + " and createdAt >= " + timestamp_from + " and createdAt <= " + timestamp_to + " ALLOW FILTERING;";
        }
        else if (upperleft_lat != "None" && upperleft_lon != "None" && lowerright_lat != "None" && lowerright_lon != "None"){
          select = "select createdAt, tweetId, userName, lang, text, geoLocationLatitude, geoLocationLongitude from master_dataset.tweets_berlin_located where geoLocationLatitude < " + upperleft_lat + " and geoLocationLatitude >= " + lowerright_lat + " and geoLocationLongitude < " + lowerright_lon + " and geoLocationLongitude >= " + upperleft_lon + " and createdAt >= " + timestamp_from + " and createdAt <= " + timestamp_to + " ALLOW FILTERING;";
        }
        else{
          select = "select createdAt, tweetId, userName, lang, text, geoLocationLatitude, geoLocationLongitude from master_dataset.tweets_berlin where createdAt >= " + timestamp_from + " and createdAt <= " + timestamp_to + " ALLOW FILTERING;";
        }
      case "Dresden" => 
        if(lang != "None"){
          select = "select createdAt, tweetId, userName, lang, text, geoLocationLatitude, geoLocationLongitude from master_dataset.tweets_dresden_by_lang where lang = " + lang + " and createdAt <= " + timestamp_to + " ALLOW FILTERING;"; 
        }
        else if (userId != "None"){
          select = "select createdAt, tweetId, userName, lang, text, geoLocationLatitude, geoLocationLongitude from master_dataset.tweets_dresden_by_user where userId = " + userId + " and createdAt >= " + timestamp_from + " and createdAt <= " + timestamp_to + " ALLOW FILTERING;";
        }
        else if (upperleft_lat != "None" && upperleft_lon != "None" && lowerright_lat != "None" && lowerright_lon != "None"){
          select = "select createdAt, tweetId, userName, lang, text, geoLocationLatitude, geoLocationLongitude from master_dataset.tweets_dresden_located where geoLocationLatitude < " + upperleft_lat + " and geoLocationLatitude >= " + lowerright_lat + " and geoLocationLongitude < " + lowerright_lon + " and geoLocationLongitude >= " + upperleft_lon + " and createdAt >= " + timestamp_from + " and createdAt <= " + timestamp_to + " ALLOW FILTERING;";
        }
        else{
          select = "select createdAt, tweetId, userName, lang, text, geoLocationLatitude, geoLocationLongitude from master_dataset.tweets_dresden where createdAt >= " + timestamp_from + " and createdAt <= " + timestamp_to + " ALLOW FILTERING;";
        }
      case "Munich" => 
        if(lang != "None"){
          select = "select createdAt, tweetId, userName, lang, text, geoLocationLatitude, geoLocationLongitude from master_dataset.tweets_munich_by_lang where lang = " + lang + " and createdAt <= " + timestamp_to + " ALLOW FILTERING;"; 
        }
        else if (userId != "None"){
          select = "select createdAt, tweetId, userName, lang, text, geoLocationLatitude, geoLocationLongitude from master_dataset.tweets_munich_by_user where userId = " + userId + " and createdAt >= " + timestamp_from + " and createdAt <= " + timestamp_to + " ALLOW FILTERING;";
        }
        else if (upperleft_lat != "None" && upperleft_lon != "None" && lowerright_lat != "None" && lowerright_lon != "None"){
          select = "select createdAt, tweetId, userName, lang, text, geoLocationLatitude, geoLocationLongitude from master_dataset.tweets_munich_located where geoLocationLatitude < " + upperleft_lat + " and geoLocationLatitude >= " + lowerright_lat + " and geoLocationLongitude < " + lowerright_lon + " and geoLocationLongitude >= " + upperleft_lon + " and createdAt >= " + timestamp_from + " and createdAt <= " + timestamp_to + " ALLOW FILTERING;";
        }
        else{
          select = "select createdAt, tweetId, userName, lang, text, geoLocationLatitude, geoLocationLongitude from master_dataset.tweets_munich where createdAt >= " + timestamp_from + " and createdAt <= " + timestamp_to + " ALLOW FILTERING;";
        }
        
    }
    val result = cassandraConn.execute(select).all().toList;
    val tweets: ListBuffer[TweetSummary] = ListBuffer()
    result.map { row =>
      tweets += TweetSummary(row.getLong("createdAt"), row.getLong("tweetId"), row.getString("userName"), row.getString("lang"), row.getString("text"), row.getDouble("geoLocationLatitude"), row.getDouble("geoLocationLongitude"))
    }
    ResponseTweets(tweets.length, tweets.toList)
  }  
}