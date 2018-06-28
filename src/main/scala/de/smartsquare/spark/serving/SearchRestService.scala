package de.smartsquare.spark.serving

import akka.actor.ActorRef
import akka.http.scaladsl.server._
import de.smartsquare.spark.util.HttpUtil
import scala.concurrent.duration.DurationInt


trait SearchRestService extends HttpUtil with SearchRestServiceHandler {


  // ==============================
  //     REST ROUTES
  // ==============================


  def getTweetsBy: Route = get {
    path("tweets" / "hamburg") {
      withRequestTimeout(120 seconds) {
        parameters('from_hour, 'to_hour, 'upperleft_lat ? "None", 'upperleft_lon ? "None", 'lowerright_lat ? "None", 'lowerright_lon ? "None", 'lang ? "None", 'userId ? "None") { (from_hour,to_hour,upperleft_lat,upperleft_lon,lowerright_lat,lowerright_lon,lang,userId) =>
          logDuration(onSuccess(getTweets(from_hour,to_hour,upperleft_lat,upperleft_lon,lowerright_lat,lowerright_lon,lang,userId, "Hamburg"))(complete(_)))
        }
      }
    }~
    path("tweets" / "berlin") {      
      withRequestTimeout(120 seconds) {
        parameters('from_hour, 'to_hour, 'upperleft_lat ? "None", 'upperleft_lon ? "None", 'lowerright_lat ? "None", 'lowerright_lon ? "None", 'lang ? "None", 'userId ? "None") { (from_hour,to_hour,upperleft_lat,upperleft_lon,lowerright_lat,lowerright_lon,lang,userId) =>
          logDuration(onSuccess(getTweets(from_hour,to_hour,upperleft_lat,upperleft_lon,lowerright_lat,lowerright_lon,lang,userId, "Berlin"))(complete(_)))
        }
      }
    }~
    path("tweets" / "dresden") {      
      withRequestTimeout(120 seconds) {
        parameters('from_hour, 'to_hour, 'upperleft_lat ? "None", 'upperleft_lon ? "None", 'lowerright_lat ? "None", 'lowerright_lon ? "None", 'lang ? "None", 'userId ? "None") { (from_hour,to_hour,upperleft_lat,upperleft_lon,lowerright_lat,lowerright_lon,lang,userId) =>
          logDuration(onSuccess(getTweets(from_hour,to_hour,upperleft_lat,upperleft_lon,lowerright_lat,lowerright_lon,lang,userId, "Dresden"))(complete(_)))
        }
      }
    }~
    path("tweets" / "munich") {
      
      withRequestTimeout(120 seconds) {
        parameters('from_hour, 'to_hour, 'upperleft_lat ? "None", 'upperleft_lon ? "None", 'lowerright_lat ? "None", 'lowerright_lon ? "None", 'lang ? "None", 'userId ? "None") { (from_hour,to_hour,upperleft_lat,upperleft_lon,lowerright_lat,lowerright_lon,lang,userId) =>
          logDuration(onSuccess(getTweets(from_hour,to_hour,upperleft_lat,upperleft_lon,lowerright_lat,lowerright_lon,lang,userId, "Munich"))(complete(_)))
        }
      }
    }
        
  }

  def routes: Route = getTweetsBy

}