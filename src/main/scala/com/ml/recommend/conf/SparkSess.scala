package com.ml.recommend.conf

import com.ml.recommend.common.GlobalConstant
import com.ml.recommend.domain.MongoConfig
import com.ml.recommend.repository.ViolationsRepository
import com.ml.recommend.util.MongoUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait SparkSess {
  val spark:SparkSession = SparkSession.builder()
    .appName(this.getClass.getSimpleName)
    .master("local[6]")
    .getOrCreate()
  implicit val mongoConfig = MongoConfig(ViolationsRepository.mongoConfig("mongo.uri"),ViolationsRepository.mongoConfig("mongo.db"))
  val sc = spark.sparkContext
  val ssc = new StreamingContext(sc,Seconds(20))
  //加载movie数据
  val movie = MongoUtil.loadMoveDFInMongoDB(spark,GlobalConstant.MONGODB_MOVIE_COLLECTION)
  // 加载数据
  val rating = MongoUtil.loadRatingDFInMongoDB(spark,GlobalConstant.MONGODB_RATING_COLLECTION)

}
