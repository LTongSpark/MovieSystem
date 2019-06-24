package com.atguigu.offline

import com.ml.recommend.common.GlobalConstant
import com.ml.recommend.conf.SparkSess
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.mongodb.casbah.commons.MongoDBObject


object OfflineRecommender extends SparkSess{


  def main(args: Array[String]): Unit = {
    val ratingExis =MongoClient(MongoClientURI(mongoConfig.uri))(mongoConfig.db)(GlobalConstant.MONGODB_RATING_COLLECTION)
      .find(MongoDBObject("uid" ->1))
      .toArray
      .foreach(println)




      //.toDF("mid", "name", "genres").cache()
  }

}

