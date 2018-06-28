package de.smartsquare.spark.serving

import akka.util.Timeout
import de.smartsquare.spark.kafka.consumer.CassandraOperation
import de.smartsquare.spark.util.{JsonHelper, LoggerUtil}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

trait SearchRestServiceHandler extends JsonHelper  with LoggerUtil {

  implicit val timeout = Timeout(120 seconds)

  def getTweets(from_hour: String,to_hour: String,upperleft_lat: String,upperleft_lon: String,lowerright_lat: String,lowerright_lon: String,lang: String,userId: String, city: String): Future[String] = {
    Future(write(CassandraOperation.getTweets(from_hour, to_hour, upperleft_lat, upperleft_lon, lowerright_lat, lowerright_lon, lang, userId, city)))
  }

}

object SearchRestServiceHandler extends SearchRestServiceHandler