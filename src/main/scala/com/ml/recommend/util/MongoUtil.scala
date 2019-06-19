package com.ml.recommend.util

import com.ml.recommend.common.GlobalConstant
import com.ml.recommend.domain.{MongoConfig, Movie, MovieRecs, Rating}
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @author LTong
  * @date 2019-06-18 下午 12:51
  */
object MongoUtil {

  def loadRatingDFInMongoDB(spark:SparkSession, collection_name: String)(implicit mongoConfig: MongoConfig): DataFrame ={
    import spark.implicits._
    spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()
  }

  def loadMoveDFInMongoDB(spark:SparkSession, collection_name: String)(implicit mongoConfig: MongoConfig): DataFrame ={
    import spark.implicits._
    spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()
  }


  def loadMovieRecDFInMongoDB(spark:SparkSession, collection_name: String)(implicit mongoConfig: MongoConfig):collection.Map[Int, Map[Int, Double]] ={
    import spark.implicits._
    spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map{ movieRecs => // 为了查询相似度方便，转换成map
        (movieRecs.mid, movieRecs.recs.map( x=> (x.mid, x.score) ).toMap )
      }.collectAsMap()
  }


  def insertDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit ={
    //新建一个mongodb的链接
    val mongoClient:MongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    //如果mongodb中已经有相应的数据库 ，先删除

    mongoClient(mongoConfig.db)(collection_name).dropCollection()
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

  /**
    * 保存数据到mongodb中
    * @param movieDF
    * @param ratingDF
    * @param tagDF
    */
  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {

    //新建一个mongodb的链接
    val mongoClient:MongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    //如果mongodb中已经有相应的数据库 ，先删除

    mongoClient(mongoConfig.db)(GlobalConstant.MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(GlobalConstant.MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(GlobalConstant.MONGODB_TAG_COLLECTION).dropCollection()

    //将对应数据写入对应的mongodb中
    movieDF.write.option("uri",mongoConfig.uri)
      .option("collection" ,GlobalConstant.MONGODB_MOVIE_COLLECTION)
      .mode(SaveMode.Overwrite)
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri" ,mongoConfig.uri)
      .option("collection" ,GlobalConstant.MONGODB_RATING_COLLECTION)
      .mode(SaveMode.Overwrite)
      .format("com.mongodb.spark.sql")
      .save()

    tagDF.write
      .option("uri" ,mongoConfig.uri)
      .option("collection" ,GlobalConstant.MONGODB_TAG_COLLECTION)
      .mode(SaveMode.Overwrite)
      .format("com.mongodb.spark.sql")
      .save()

    //对数据表建索引
    mongoClient(mongoConfig.db)(GlobalConstant.MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(GlobalConstant.MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(GlobalConstant.MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(GlobalConstant.MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(GlobalConstant.MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    mongoClient.close()

  }

  /**
    * 将推荐结果保存
    * @param uid
    * @param streamRecs
    * @param mongoConfig
    */
  def saveDataToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit ={
    // 定义到StreamRecs表的连接
    val streamRecsCollection = MongoClient(MongoClientURI(mongoConfig.uri))(mongoConfig.db)(GlobalConstant.MONGODB_STREAM_RECS_COLLECTION)

    // 如果表中已有uid对应的数据，则删除
    streamRecsCollection.findAndRemove( MongoDBObject("uid" -> uid) )
    // 将streamRecs数据存入表中
    streamRecsCollection.insert( MongoDBObject( "uid"->uid,
      "recs"-> streamRecs.map(x=>MongoDBObject( "mid"->x._1, "score"->x._2 )) ) )
  }

}
