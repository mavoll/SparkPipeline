package de.smartsquare.spark.kafka.producer

import java.util.Date
import java.util.concurrent.LinkedBlockingQueue
import java.text._

import scala.collection.mutable.ListBuffer

import de.smartsquare.spark.util.{JsonHelper, Tweet, UserMentionEntityCaseClass, URLEntityCaseClass, HashtagEntityCaseClass, MediaEntityCaseClass, ExtendedMediaEntityCaseClass, VariantCaseClass, SymbolEntityCaseClass, GeoLocationCaseClass}
import de.smartsquare.spark.util.SparkPipelineConfigUtil._
import twitter4j._
import twitter4j.TwitterFactory
import twitter4j.conf.ConfigurationBuilder

object TwitterStreamApp extends App with JsonHelper{


  val queue = new LinkedBlockingQueue[Status](1000)
 
  val boundingBox: Array[Double] = Array(boundingBox1SouthWestLon,boundingBox1SouthWestLat) //southwest
  val boundingBox1: Array[Double] = Array(boundingBoxNorthEastLon,boundingBoxNorthEastLat) //northeast

  val keyWords = twitterKeyWords.toArray

  val cb = new ConfigurationBuilder()
  cb.setDebugEnabled(true)
    .setOAuthConsumerKey(consumerKey)
    .setOAuthConsumerSecret(consumerSecret)
    .setOAuthAccessToken(accessToken)
    .setOAuthAccessTokenSecret(accessTokenSecret)

  val twitterStream = new TwitterStreamFactory(cb.build()).getInstance()
  var counter = 0
  val listener = new StatusListener() {
    override def onStatus(status: Status) {

      queue.offer(status)
      counter = counter + 1
      
      var year: Option[Int] = None
      var month: Option[Int] = None
      var day: Option[Int] = None
      var hour: Option[Int] = None      
      var tweetId: Option[Long] = None    
      var userId: Option[Long] = None   
      var createdAt: Option[Long] = None    
      var currentUserRetweetId: Option[Long] = None
      var favoriteCount: Option[Long] = None
      
      var userName: Option[String] = None        
      var retweetedStatusId: Option[Long] = None    
      var retweetedStatusCreatedAt: Option[Long] = None   
      var retweetedStatusUserId: Option[Long] = None
      var retweetedStatusUserName: Option[String] = None
      
      var retweetCount: Option[Long] = None
      var source: Option[String] = None
      var text: Option[String] = None
      
      var inReplyToScreenName: Option[String] = None
      var inReplyToStatusId: Option[Long] = None 
      var inReplyToUserId: Option[Long] = None
      var lang: Option[String] = None
      var quotedStatusId: Option[Long] = None                         
                                
      var quotedStatusCreatedAt: Option[Long] = None
      var quotedStatusUserId: Option[Long] = None
      var quotedStatusUserName: Option[String] = None   
      
      var isFavorited: Option[Boolean] = None
      var isPossiblySensitive: Option[Boolean] = None
      var isRetweet: Option[Boolean] = None
      var isRetweeted: Option[Boolean] = None
      var isRetweetedByMe: Option[Boolean] = None
      var isTruncated: Option[Boolean] = None
      
      var placeCountry: Option[String] = None
      var placeStreetAddress: Option[String] = None
      var placeName: Option[String] = None
      var placeId: Option[String] = None
      var placeFullName: Option[String] = None
      var placeURL: Option[String] = None
      var placeCountryCode: Option[String] = None
      var placePlaceType: Option[String] = None
      var placeBoundingBoxType: Option[String] = None
      var placeGeometryType: Option[String] = None   
      var placeBoundingBoxCoordinatesList: Option[List[GeoLocationCaseClass]] = None
      var placeGeometryCoordinatesList: Option[List[GeoLocationCaseClass]] = None    
      
      var latitude: Option[Double] = None     
      var longitude: Option[Double] = None   
      var listContributors: Option[List[Long]] = None
      var listWithheldInCountries: Option[List[String]] = None
      var listUserWithheldInCountries: Option[List[String]] = None
      
      var userLang: Option[String] = None  
      var userDescription: Option[String] = None
      var userListedCount: Option[Long] = None
      var userLocation: Option[String] = None      
      var userProfileImageURL: Option[String] = None 
      var userProfileImageURLHttps: Option[String] = None 
      var userScreenName: Option[String] = None
  //    var userStatus,   
      var userTimeZone: Option[String] = None 
      var userURL: Option[String] = None 
      var userURLEntityDisplayURL: Option[String] = None
      var userURLEntityEnd: Option[Int] = None 
      var userURLEntityExpandedURL: Option[String] = None
      var userURLEntityStart: Option[Int] = None
      var userURLEntityText: Option[String] = None
      var userURLEntityURL: Option[String] = None
      var userUtcOffset: Option[Int] = None
      
      var userIsContributorsEnabled: Option[Boolean] = None
      var userIsDefaultProfile: Option[Boolean] = None
      var userIsDefaultProfileImage: Option[Boolean] = None
      var userIsFollowRequestSent: Option[Boolean] = None
      var userIsGeoEnabled: Option[Boolean] = None
      var userIsProtected: Option[Boolean] = None
      var userIsShowAllInlineMedia: Option[Boolean] = None
      var userIsTranslator: Option[Boolean] = None
      var userIsVerified: Option[Boolean] = None
      var userFriendsCount: Option[Long] = None
      var userFavouritesCount: Option[Long] = None
      var userFollowersCount: Option[Long] = None
      var userStatusesCount: Option[Long] = None      
      
      var userMentionEntitiesList: Option[List[UserMentionEntityCaseClass]] = None
      var uRLEntitiesList: Option[List[URLEntityCaseClass]] = None
      var hashtagEntitiesList: Option[List[HashtagEntityCaseClass]] = None
      var mediaEntitiesList: Option[List[MediaEntityCaseClass]] = None
      //extendedMediaEntitiesList: Option[List[ExtendedMediaEntityCaseClass]] = None
      var symbolEntitiesList: Option[List[SymbolEntityCaseClass]] = None
                              
      tweetId = Some(status.getId)  
      userId = Some(status.getUser.getId)
      userName = Some(Option(status.getUser.getName).getOrElse("").replaceAll("'","''")) 
      createdAt = Some(status.getCreatedAt.getTime)
      val yearFormat = new SimpleDateFormat("yyyy")
      val monthFormat = new SimpleDateFormat("MM")
      val dayFormat = new SimpleDateFormat("dd")   
      val hourFormat = new SimpleDateFormat("HH")           
      year = Some(Integer.parseInt(yearFormat.format(createdAt.getOrElse(""))))
      month = Some(Integer.parseInt(monthFormat.format(createdAt.getOrElse(""))))
      day = Some(Integer.parseInt(dayFormat.format(createdAt.getOrElse(""))))
      hour = Some(Integer.parseInt(hourFormat.format(createdAt.getOrElse("")))) 
      currentUserRetweetId = Some(status.getCurrentUserRetweetId)
      favoriteCount = Some(status.getFavoriteCount)
      inReplyToScreenName = Some(Option(status.getInReplyToScreenName).getOrElse("").replaceAll("'","''"))
      inReplyToStatusId = Some(status.getInReplyToStatusId) 
      inReplyToUserId = Some(status.getInReplyToUserId)
      lang = Some(Option(status.getLang).getOrElse("").replaceAll("'","''"))
      quotedStatusId = Some(status.getQuotedStatusId) 
            
      if (status.getRetweetedStatus != null){
        retweetedStatusId = Some(status.getRetweetedStatus.getId)
        retweetedStatusCreatedAt = Some(status.getRetweetedStatus.getCreatedAt.getTime)
        retweetedStatusUserId = Some(status.getRetweetedStatus.getUser.getId)
        retweetedStatusUserName = Some(Option(status.getRetweetedStatus.getUser.getName).getOrElse("").replaceAll("'","''")) 
      }      
      
      retweetCount = Some(status.getRetweetCount)
      source = Some(Option(status.getSource).getOrElse("").replaceAll("'","''")) 
      text = Some(Option(status.getText).getOrElse("").replaceAll("'","''"))        
      
      if (status.getQuotedStatus != null){
        quotedStatusCreatedAt = Some(status.getQuotedStatus.getCreatedAt.getTime)
        quotedStatusUserId = Some(status.getQuotedStatus.getUser.getId)
        quotedStatusUserName = Some(Option(status.getQuotedStatus.getUser.getName).getOrElse("").replaceAll("'","''"))  
      }     
      
      isFavorited = Some(status.isFavorited)
      isPossiblySensitive = Some(status.isPossiblySensitive)
      isRetweet = Some(status.isRetweet)
      isRetweeted = Some(status.isRetweeted)
      isRetweetedByMe = Some(status.isRetweetedByMe)
      isTruncated = Some(status.isTruncated)
                 
      val listBoundingBoxCoordinates =  new ListBuffer[GeoLocationCaseClass]() 
      val listGeometryCoordinates =  new ListBuffer[GeoLocationCaseClass]() 
      
      if (status.getPlace != null){
        placeCountry = Some(Option(status.getPlace.getCountry).getOrElse("").replaceAll("'","''"))
        placeStreetAddress = Some(Option(status.getPlace.getStreetAddress).getOrElse("").replaceAll("'","''"))
        placeName = Some(Option(status.getPlace.getName).getOrElse("").replaceAll("'","''"))
        placeId = Some(Option(status.getPlace.getId).getOrElse("").replaceAll("'","''"))
        placeFullName = Some(Option(status.getPlace.getFullName).getOrElse("").replaceAll("'","''"))
        placeURL = Some(Option(status.getPlace.getURL).getOrElse("").replaceAll("'","''"))
        placeCountryCode = Some(Option(status.getPlace.getCountryCode).getOrElse("").replaceAll("'","''"))
        placePlaceType = Some(Option(status.getPlace.getPlaceType).getOrElse("").replaceAll("'","''"))
        placeBoundingBoxType = Some(Option(status.getPlace.getBoundingBoxType).getOrElse("").replaceAll("'","''"))
        placeGeometryType = Some(Option(status.getPlace.getGeometryType).getOrElse("").replaceAll("'","''"))
        
        if (status.getPlace.getBoundingBoxCoordinates != null){
          val tempBoundingBoxCoordinates = status.getPlace.getBoundingBoxCoordinates.toList
          tempBoundingBoxCoordinates.foreach{i =>
            i.foreach{geolocation =>
              listBoundingBoxCoordinates.append(GeoLocationCaseClass(Some(geolocation.getLongitude), Some(geolocation.getLatitude)))
            }            
          }                                          
          placeBoundingBoxCoordinatesList = Some(listBoundingBoxCoordinates.toList)      
        }
        
        if (status.getPlace.getGeometryCoordinates != null){
          val tmpGeometryCoordinates = status.getPlace.getGeometryCoordinates.toList
          tmpGeometryCoordinates.foreach{i =>
            i.foreach{geolocation =>
              listGeometryCoordinates.append(GeoLocationCaseClass(Some(geolocation.getLongitude), Some(geolocation.getLatitude)))
            }            
          }      
        }
        placeGeometryCoordinatesList = Some(listGeometryCoordinates.toList)
      }      
      
      if (status.getGeoLocation != null){
        latitude = Some(status.getGeoLocation.getLatitude)
        longitude = Some(status.getGeoLocation.getLongitude)
      }
      
      if (status.getContributors != null){
        listContributors = Some(status.getContributors.toList)
      }      
      
      if (status.getWithheldInCountries != null){
        listWithheldInCountries = Some(status.getWithheldInCountries.toList)
      }
      
      userLang = Some(Option(status.getUser.getLang).getOrElse("").replaceAll("'","''"))  
      userDescription = Some(Option(status.getUser.getDescription).getOrElse("").replaceAll("'","''"))
      userListedCount = Some(status.getUser.getListedCount)
      userLocation = Some(Option(status.getUser.getLocation).getOrElse("").replaceAll("'","''"))
      userProfileImageURL = Some(Option(status.getUser.getProfileImageURL).getOrElse("").replaceAll("'","''"))
      userProfileImageURLHttps = Some(Option(status.getUser.getProfileImageURLHttps).getOrElse("").replaceAll("'","''")) 
      userScreenName = Some(Option(status.getUser.getScreenName).getOrElse("").replaceAll("'","''"))
      userTimeZone = Some(Option(status.getUser.getTimeZone).getOrElse("").replaceAll("'","''")) 
      userURL = Some(Option(status.getUser.getURL).getOrElse("").replaceAll("'","''")) 
      userURLEntityDisplayURL = Some(Option(status.getUser.getURLEntity.getDisplayURL).getOrElse("").replaceAll("'","''")) 
      userURLEntityEnd = Some(status.getUser.getURLEntity.getEnd) 
      userURLEntityExpandedURL = Some(Option(status.getUser.getURLEntity.getExpandedURL).getOrElse("").replaceAll("'","''")) 
      userURLEntityStart = Some(status.getUser.getURLEntity.getStart) 
      userURLEntityText = Some(Option(status.getUser.getURLEntity.getText).getOrElse("").replaceAll("'","''")) 
      userURLEntityURL = Some(Option(status.getUser.getURLEntity.getURL).getOrElse("").replaceAll("'","''")) 
      userUtcOffset = Some(status.getUser.getUtcOffset)      
      
      if (status.getUser.getWithheldInCountries != null){
        listUserWithheldInCountries = Some(status.getUser.getWithheldInCountries.toList)
      }      
      
      userIsContributorsEnabled = Some(status.getUser.isContributorsEnabled)
      userIsDefaultProfile = Some(status.getUser.isDefaultProfile)
      userIsDefaultProfileImage = Some(status.getUser.isDefaultProfileImage) 
      userIsFollowRequestSent = Some(status.getUser.isFollowRequestSent)      
      userIsGeoEnabled = Some(status.getUser.isGeoEnabled)
      userIsProtected = Some(status.getUser.isProtected)
      userIsShowAllInlineMedia =  Some(status.getUser.isShowAllInlineMedia)
      userIsTranslator = Some(status.getUser.isTranslator)
      userIsVerified = Some(status.getUser.isVerified) 
      userFriendsCount = Some(status.getUser.getFriendsCount)
      userFavouritesCount = Some(status.getUser.getFavouritesCount)
      userFollowersCount = Some(status.getUser.getFollowersCount)
      userStatusesCount = Some(status.getUser.getStatusesCount)
                        
      val listUME =  new ListBuffer[UserMentionEntityCaseClass]() 
      if (status.getUserMentionEntities != null){
        val userMentionEntities = status.getUserMentionEntities
        userMentionEntities.foreach{i =>listUME.append(UserMentionEntityCaseClass(Some(i.getEnd), Some(i.getId), Some(Option(i.getName).getOrElse("").replaceAll("'","''")), Some(Option(i.getScreenName).getOrElse("").replaceAll("'","''")), Some(i.getStart), Some(Option(i.getText).getOrElse("").replaceAll("'","''"))))}
      }
      userMentionEntitiesList = Some(listUME.toList)
        
      val listURLE =  new ListBuffer[URLEntityCaseClass]()
      if (status.getURLEntities != null){      
        val uRLEntities = status.getURLEntities
        uRLEntities.foreach{i =>listURLE.append(URLEntityCaseClass(Some(Option(i.getDisplayURL).getOrElse("").replaceAll("'","''")), Some(i.getEnd), Some(Option(i.getExpandedURL).getOrElse("").replaceAll("'","''")), Some(i.getStart), Some(Option(i.getText).getOrElse("").replaceAll("'","''")), Some(Option(i.getURL).getOrElse("").replaceAll("'","''"))))}
      }
      uRLEntitiesList = Some(listURLE.toList)
       
      val listHE =  new ListBuffer[HashtagEntityCaseClass]()
      if (status.getHashtagEntities != null){      
        val hashtagEntities = status.getHashtagEntities
        hashtagEntities.foreach{i =>listHE.append(HashtagEntityCaseClass(Some(i.getEnd), Some(i.getStart), Some(Option(i.getText).getOrElse("").replaceAll("'","''"))))}
      }
      hashtagEntitiesList = Some(listHE.toList)
            
      val listME =  new ListBuffer[MediaEntityCaseClass]()
      if (status.getMediaEntities != null){     
        val mediaEntities = status.getMediaEntities
        mediaEntities.foreach{i =>listME.append(MediaEntityCaseClass(Some(Option(i.getDisplayURL).getOrElse("").replaceAll("'","''")), Some(i.getEnd), Some(Option(i.getExpandedURL).getOrElse("").replaceAll("'","''")), Some(i.getStart), Some(Option(i.getText).getOrElse("").replaceAll("'","''")), Some(Option(i.getURL).getOrElse("").replaceAll("'","''")), Some(i.getId), Some(Option(i.getMediaURL).getOrElse("").replaceAll("'","''")), Some(Option(i.getMediaURLHttps).getOrElse("").replaceAll("'","''")), Some(Option(i.getType).getOrElse("").replaceAll("'","''"))))}
      }     
      mediaEntitiesList = Some(listME.toList)
      
      val listSE =  new ListBuffer[SymbolEntityCaseClass]()
      if (status.getSymbolEntities != null){  
        val symbolEntities = status.getSymbolEntities
        symbolEntities.foreach{i =>listSE.append(SymbolEntityCaseClass(Some(i.getEnd),  Some(i.getStart),  Some(Option(i.getText).getOrElse("").replaceAll("'","''"))))}
      } 
      symbolEntitiesList = Some(listSE.toList)
      
      val message = write(Tweet(                               
                                year,                              
                                month,                              
                                day,                               
                                hour,                             
                                tweetId,
                                createdAt,
                                currentUserRetweetId,
                                favoriteCount,
                                listContributors,     
                                inReplyToScreenName,
                                inReplyToStatusId, 
                                inReplyToUserId,
                                lang,
                                quotedStatusId,                                 
                                quotedStatusCreatedAt,
                                quotedStatusUserId,
                                quotedStatusUserName,    
                                retweetedStatusId,
                                retweetedStatusCreatedAt,
                                retweetedStatusUserId,
                                retweetedStatusUserName,    
                                retweetCount, 
                                source,
                                text,                                
                                listWithheldInCountries,                                   
                                isFavorited,
                                isPossiblySensitive,
                                isRetweet,
                                isRetweeted,
                                isRetweetedByMe,
                                isTruncated,  
                                placeCountry,
                                placeStreetAddress,
                                placeName,
                                placeId,
                                placeFullName,
                                placeURL,
                                placeCountryCode, 
                                placePlaceType,
                                placeBoundingBoxType,
                                placeGeometryType,
                                placeBoundingBoxCoordinatesList,
                                placeGeometryCoordinatesList,
                                latitude,
                                longitude,      
                                userId,
                                userName,   
                                userLang,  
                                userDescription,
                                userListedCount,
                                userLocation,
                                userProfileImageURL,
                                userProfileImageURLHttps,
                                userScreenName,
                                userTimeZone,
                                userURL,
                                userURLEntityDisplayURL,
                                userURLEntityEnd,
                                userURLEntityExpandedURL,
                                userURLEntityStart,
                                userURLEntityText,
                                userURLEntityURL,
                                userUtcOffset,                                
                                listUserWithheldInCountries,                                 
                                userIsContributorsEnabled,
                                userIsDefaultProfile,
                                userIsDefaultProfileImage,
                                userIsFollowRequestSent,  
                                userIsGeoEnabled,
                                userIsProtected,
                                userIsShowAllInlineMedia,
                                userIsTranslator,
                                userIsVerified,
                                userFriendsCount,
                                userFavouritesCount,
                                userFollowersCount,
                                userStatusesCount,     
                                userMentionEntitiesList,
                                uRLEntitiesList,
                                hashtagEntitiesList,
                                mediaEntitiesList,
                                //Some(listEME.toList),
                                symbolEntitiesList
                                                          
                             )
                          )
      KafkaTwitterProducer.send(kafkaTopic(0),message)
      println(s" tweet text is ::::   ${message}  counter ::: ${counter}")
  
    }

    override def onDeletionNotice(statusDeletion_Notice: StatusDeletionNotice) = {

    }

    override def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {
      // System.out.println("Got track limitation notice:" +num-berOfLimitedStatuses);
    }

    override def onScrubGeo(userId: Long, upToStatusId: Long) {
      // System.out.println("Got scrub_geo event userId:" + userId +"upToStatusId:" + upToStatusId);
    }

    override def onStallWarning(warning: StallWarning) {
      // System.out.println("Got stall warning:" + warning);
    }

    override def onException(ex: Exception) {
      ex.printStackTrace()
    }
  }
  twitterStream.addListener(listener)

  val query = new FilterQuery()
  query.track(keyWords:_*)
  query.locations(boundingBox, boundingBox1)
  twitterStream.filter(query)


  Thread.sleep(100000000)
  //twitterStream.cleanUp();
  //twitterStream.shutdown();

}   