package de.smartsquare.spark.kafka.consumer

import com.datastax.driver.core._
import de.smartsquare.spark.util.SparkPipelineConfigUtil._
import org.slf4j.LoggerFactory

trait CassandraConnection {

  val logger = LoggerFactory.getLogger(getClass.getName)
  val defaultConsistencyLevel = ConsistencyLevel.valueOf(writeConsistency)
  val cassandraConn: Session = {
    val cluster = new Cluster.Builder().withClusterName(clusterName).
      addContactPoints(hosts.toArray: _*).
      withPort(port).
      withCredentials(username, password).
      withQueryOptions(new QueryOptions().setConsistencyLevel(defaultConsistencyLevel)).build
    val session = cluster.connect
    session.execute(s"USE ${cassandraKeyspaces(0)}")
    
    //SEE ZEPPELIN FOR NEW VERSION
    /*session.execute(s"CREATE KEYSPACE IF NOT EXISTS  ${cassandraKeyspaces.get(0)} WITH REPLICATION = " +
      s"{ 'class' : 'SimpleStrategy', 'replication_factor' : ${replicationFactor} }")
    session.execute(s"USE ${cassandraKeyspaces.get(0)}")
    
    val query = s"CREATE TABLE IF NOT EXISTS tweets_hamburg " +
      s"(year int, month int, day int, hour int, tweetId bigint, createdAt bigint, currentUserRetweetId bigint, " +
      s"favoriteCount bigint, contributors text, inReplyToScreenName text, " +
      s"inReplyToStatusId bigint, inReplyToUserId bigint, lang text, " +
      s"quotedStatusId bigint, retweetCount bigint, source text, " +
      s"text text, withheldInCountries text, isFavorited boolean, " +
      s"isPossiblySensitive boolean, isRetweet boolean, isRetweeted boolean, " +
      s"isRetweetedByMe boolean, isTruncated boolean, placeCountry text, " +
      s"placeStreetAddress text, placeName text, placeId text, " +
      s"placeFullName text, placeURL text, placeCountryCode text, " +
      s"placeType text, placeBoundingBoxType text, placeGeometryType text, " +
      s"placeBoundingBoxCoordinates text, placeGeometryCoordinates text, geoLocationLatitude double, " +
      s"geoLocationLongitude double, userId bigint, userName text, " +
      s"userLang text, userDescription text, userListedCount bigint, " +
      s"userLocation text, userProfileImageURL text, userProfileImageURLHttps text, " +
      s"userScreenName text, userTimeZone text, userURL text, " +
      s"userURLEntityDisplayURL text, userURLEntityEnd bigint, userURLEntityExpandedURL text, " +
      s"userURLEntityStart bigint, userURLEntityText text, userURLEntityURL text, " +
      s"userUtcOffset bigint, userWithheldInCountries text, userIsContributorsEnabled boolean, " +
      s"userIsDefaultProfile boolean, userIsDefaultProfileImage boolean, userIsFollowRequestSent boolean, " +
      s"userIsGeoEnabled boolean, userIsProtected boolean, userIsShowAllInlineMedia boolean, " +
      s"userIsTranslator boolean, userIsVerified boolean, userFriendsCount bigint, " +
      s"userFavouritesCount bigint, userFollowersCount bigint, userStatusesCount bigint, " +
      s"userMentionEntities text, uRLEntities text, hashtagEntities text, " +
      s"mediaEntities text, symbolEntities text, " +
      s" PRIMARY KEY ((year, month, day), createdAt, tweetId)) WITH CLUSTERING ORDER BY (createdAt DESC)"
      
    val query1 = s"CREATE TABLE IF NOT EXISTS tweets_counter " +
      s"(table_name text, tweets_counter counter, " +
      s" PRIMARY KEY (table_name)) "
      
    val query2 = s"CREATE TABLE IF NOT EXISTS tweets_counter_hourly " +
      s"(table_name text, year int, month int, day int, hour int, tweets_counter counter, " +
      s" PRIMARY KEY (table_name, year, month, day, hour)) "
      
    val query3 = s"CREATE TABLE IF NOT EXISTS tweets_counter_monthly " +
      s"(table_name text, year int, month int, tweets_counter counter, " +
      s" PRIMARY KEY (table_name, year, month)) "

    createTables(session, query)
    createTables(session, query1)
    createTables(session, query2)
    createTables(session, query3)*/
    session
  }

  val cassandraConn1: Session = {
    val cluster = new Cluster.Builder().withClusterName(clusterName).
      addContactPoints(hosts.toArray: _*).
      withPort(port).
      withCredentials(username, password).
      withQueryOptions(new QueryOptions().setConsistencyLevel(defaultConsistencyLevel)).build
    val session = cluster.connect
    
    //SEE ON ZEPPELIN
    /*session.execute(s"CREATE KEYSPACE IF NOT EXISTS  ${cassandraKeyspaces.get(1)} WITH REPLICATION = " +
      s"{ 'class' : 'SimpleStrategy', 'replication_factor' : ${replicationFactor} }")
    session.execute(s"USE ${cassandraKeyspaces.get(1)}")
    
    val query = s"CREATE TABLE IF NOT EXISTS tweets_hamburg_filtered " +
      s"(year int, month int, day int, hour int, tweetId bigint, createdAt bigint, currentUserRetweetId bigint, " +
      s"favoriteCount bigint, contributors text, inReplyToScreenName text, " +
      s"inReplyToStatusId bigint, inReplyToUserId bigint, lang text, " +
      s"quotedStatusId bigint, retweetCount bigint, source text, " +
      s"text text, withheldInCountries text, isFavorited boolean, " +
      s"isPossiblySensitive boolean, isRetweet boolean, isRetweeted boolean, " +
      s"isRetweetedByMe boolean, isTruncated boolean, placeCountry text, " +
      s"placeStreetAddress text, placeName text, placeId text, " +
      s"placeFullName text, placeURL text, placeCountryCode text, " +
      s"placeType text, placeBoundingBoxType text, placeGeometryType text, " +
      s"placeBoundingBoxCoordinates text, placeGeometryCoordinates text, geoLocationLatitude double, " +
      s"geoLocationLongitude double, userId bigint, userName text, " +
      s"userLang text, userDescription text, userListedCount bigint, " +
      s"userLocation text, userProfileImageURL text, userProfileImageURLHttps text, " +
      s"userScreenName text, userTimeZone text, userURL text, " +
      s"userURLEntityDisplayURL text, userURLEntityEnd bigint, userURLEntityExpandedURL text, " +
      s"userURLEntityStart bigint, userURLEntityText text, userURLEntityURL text, " +
      s"userUtcOffset bigint, userWithheldInCountries text, userIsContributorsEnabled boolean, " +
      s"userIsDefaultProfile boolean, userIsDefaultProfileImage boolean, userIsFollowRequestSent boolean, " +
      s"userIsGeoEnabled boolean, userIsProtected boolean, userIsShowAllInlineMedia boolean, " +
      s"userIsTranslator boolean, userIsVerified boolean, userFriendsCount bigint, " +
      s"userFavouritesCount bigint, userFollowersCount bigint, userStatusesCount bigint, " +
      s"userMentionEntities text, uRLEntities text, hashtagEntities text, " +
      s"mediaEntities text, symbolEntities text, filterKeywords text, " +
      s" PRIMARY KEY ((year, month, day), createdAt, tweetId)) WITH CLUSTERING ORDER BY (createdAt DESC)"
      
    val query1 = s"CREATE TABLE IF NOT EXISTS tweets_counter_filtered " +
      s"(table_name text, tweets_counter counter, " +
      s" PRIMARY KEY (table_name)) "
      
    val query2 = s"CREATE TABLE IF NOT EXISTS tweets_counter_hourly_filtered " +
      s"(table_name text, year int, month int, day int, hour int, tweets_counter counter, " +
      s" PRIMARY KEY (table_name, year, month, day, hour)) "
      
    val query3 = s"CREATE TABLE IF NOT EXISTS tweets_counter_monthly_filtered " +
      s"(table_name text, year int, month int, tweets_counter counter, " +
      s" PRIMARY KEY (table_name, year, month)) "
      
    val query4 = s"CREATE TABLE IF NOT EXISTS filter_keywords " +
      s"(type text, keyword text, " +
      s" PRIMARY KEY (type, keyword)) "

    createTables(session, query)
    createTables(session, query1)
    createTables(session, query2)
    createTables(session, query3)
    createTables(session, query4)*/
    session
  }

  val cassandraConn2: Session = {
    val cluster = new Cluster.Builder().withClusterName(clusterName).
      addContactPoints(hosts.toArray: _*).
      withPort(port).
      withCredentials(username, password).
      withQueryOptions(new QueryOptions().setConsistencyLevel(defaultConsistencyLevel)).build
    val session = cluster.connect
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS  ${cassandraKeyspaces(2)} WITH REPLICATION = " +
      s"{ 'class' : 'SimpleStrategy', 'replication_factor' : ${replicationFactor} }")
    session.execute(s"USE ${cassandraKeyspaces(2)}")
    val query = s"CREATE TABLE IF NOT EXISTS friendcountview " +
      s"(createdAt bigint, userId bigint, " + "friendsCount bigint, " +
      s" PRIMARY KEY (userId, createdAt)) "
    createTables(session, query)
    session
  }


  def createTables(session: Session, createTableQuery: String): ResultSet = session.execute(createTableQuery)

}

object CassandraConnection extends CassandraConnection
