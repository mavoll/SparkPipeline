package de.smartsquare.spark.util

case class Tweet(
     
    year: Option[Int] = None,
    month: Option[Int] = None,
    day: Option[Int] = None,
    hour: Option[Int] = None,
    tweetId: Option[Long] = None,
    createdAt: Option[Long] = None,
    currentUserRetweetId: Option[Long] = None, 
    favoriteCount: Option[Long] = None, 
    contributors: Option[List[Long]] = None,      
    inReplyToScreenName: Option[String] = None, 
    inReplyToStatusId: Option[Long] = None, 
    inReplyToUserId: Option[Long] = None,
    lang: Option[String] = None,
//    quotedStatus, 
    quotedStatusId: Option[Long] = None,
    quotedStatusCreatedAt: Option[Long] = None,
    quotedStatusUserId: Option[Long] = None,
    quotedStatusUserName: Option[String] = None,    
    retweetedStatusId: Option[Long] = None,
    retweetedStatusCreatedAt: Option[Long] = None,
    retweetedStatusUserId: Option[Long] = None,
    retweetedStatusUserName: Option[String] = None,    
    retweetCount: Option[Long] = None, 
//    retweetedStatus: Status,  
//    scopes: Scopes, 
    source: Option[String] = None, 
    text: Option[String] = None, 
    withheldInCountries: Option[List[String]] = None, 
    isFavorited: Option[Boolean] = None, 
    isPossiblySensitive: Option[Boolean] = None, 
    isRetweet: Option[Boolean] = None, 
    isRetweeted: Option[Boolean] = None,
    isRetweetedByMe: Option[Boolean] = None,  
    isTruncated: Option[Boolean] = None, 
    placeCountry: Option[String] = None,
    placeStreetAddress: Option[String] = None,
    placeName: Option[String] = None,
    placeId: Option[String] = None,
    placeFullName: Option[String] = None,
    placeURL: Option[String] = None,
    placeCountryCode: Option[String] = None, 
    placeType: Option[String] = None,  
    placeBoundingBoxType: Option[String] = None,
    placeGeometryType: Option[String] = None,
    placeBoundingBoxCoordinates: Option[List[GeoLocationCaseClass]] = None,
    placeGeometryCoordinates: Option[List[GeoLocationCaseClass]] = None,
    geoLocationLatitude: Option[Double] = None,
    geoLocationLongitude: Option[Double] = None,    
    userId: Option[Long] = None,
    userName: Option[String] = None,   
    userLang: Option[String] = None,  
    userDescription: Option[String] = None,
    userListedCount: Option[Long] = None,
    userLocation: Option[String] = None,      
    userProfileImageURL: Option[String] = None, 
    userProfileImageURLHttps: Option[String] = None, 
    userScreenName: Option[String] = None,
//    userStatus,   
    userTimeZone: Option[String] = None, 
    userURL: Option[String] = None, 
    userURLEntityDisplayURL: Option[String] = None,
    userURLEntityEnd: Option[Int] = None, 
    userURLEntityExpandedURL: Option[String] = None,
    userURLEntityStart: Option[Int] = None, 
    userURLEntityText: Option[String] = None,
    userURLEntityURL: Option[String] = None,
    userUtcOffset: Option[Int] = None, 
    userWithheldInCountries: Option[List[String]] = None,
    userIsContributorsEnabled: Option[Boolean] = None, 
    userIsDefaultProfile: Option[Boolean] = None,
    userIsDefaultProfileImage: Option[Boolean] = None, 
    userIsFollowRequestSent: Option[Boolean] = None, 
    userIsGeoEnabled: Option[Boolean] = None, 
    userIsProtected: Option[Boolean] = None, 
    userIsShowAllInlineMedia: Option[Boolean] = None, 
    userIsTranslator: Option[Boolean] = None, 
    userIsVerified: Option[Boolean] = None,     
    userFriendsCount: Option[Long] = None,
    userFavouritesCount: Option[Long] = None,
    userFollowersCount: Option[Long] = None,
    userStatusesCount: Option[Long] = None,    
    userMentionEntities: Option[List[UserMentionEntityCaseClass]] = None,
    uRLEntities: Option[List[URLEntityCaseClass]] = None,
    hashtagEntities: Option[List[HashtagEntityCaseClass]] = None,
    mediaEntities: Option[List[MediaEntityCaseClass]] = None,
    //extendedMediaEntities: Option[List[ExtendedMediaEntityCaseClass]] = None,
    symbolEntities: Option[List[SymbolEntityCaseClass]] = None
)            
case class UserMentionEntityCaseClass(
    
    end: Option[Int] = None,
    id: Option[Long] = None,
    name: Option[String] = None,
    screenName: Option[String] = None,
    start: Option[Int] = None,
    text: Option[String] = None
)   
case class URLEntityCaseClass(
    
    displayURL: Option[String] = None,
    end: Option[Int] = None,
    expandedURL: Option[String] = None,
    start: Option[Int] = None,
    text: Option[String] = None,
    url: Option[String] = None
) 
case class HashtagEntityCaseClass(
    
    end: Option[Int] = None,
    start: Option[Int] = None,
    text: Option[String] = None
) 
case class MediaEntityCaseClass(
    
    displayURL: Option[String] = None,
    end: Option[Int] = None,
    expandedURL: Option[String] = None,
    start: Option[Int] = None,
    text: Option[String] = None,
    url: Option[String] = None,
    id: Option[Long] = None,
    mediaURL: Option[String] = None,
    mediaURLHttps: Option[String] = None,
    mediatype: Option[String] = None
) 
case class ExtendedMediaEntityCaseClass(
    
    displayURL: Option[String] = None,
    end: Option[Int] = None,
    expandedURL: Option[String] = None,
    start: Option[Int] = None,
    text: Option[String] = None,
    url: Option[String] = None,    
    id: Option[Long] = None,
    mediaURL: Option[String] = None,
    mediaURLHttps: Option[String] = None,  
    mediatype: Option[String] = None,    
    videoAspectRatioHeight: Option[Int] = None, 
    videoAspectRatioWidth: Option[Int] = None, 
    videoDurationMillis: Option[Long] = None, 
    videoVariants: Option[List[VariantCaseClass]] = None
) 
case class VariantCaseClass(
    
    bitrate: Option[Int] = None, 
    contentType: Option[String] = None, 
    url: Option[String] = None

)
case class SymbolEntityCaseClass(
    
    end: Option[Int] = None,
    start: Option[Int] = None,
    text: Option[String] = None
) 
case class GeoLocationCaseClass(
    
    longitude: Option[Double] = None,
    latitude: Option[Double] = None
) 
