package com.atguigu.offline

import com.ml.recommend.StreamingRecommend.streaming
import com.ml.recommend.common.GlobalConstant
import com.ml.recommend.domain.MongoConfig
import com.ml.recommend.repository.ViolationsRepository
import com.ml.recommend.util.MongoUtil
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}


object OfflineRecommender {


  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[6]").getOrCreate()

    implicit val mongoConfig = MongoConfig(ViolationsRepository.mongoConfig("mongo.uri"),ViolationsRepository.mongoConfig("mongo.db"))
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc,Seconds(20))

    // 加载电影相似度矩阵数据，把它广播出去
    val movieRec: collection.Map[Int,Map[Int ,Double]] = MongoUtil.loadMovieRecDFInMongoDB(spark, GlobalConstant.MOVIE_RECS)
    //问了方便查询  转换成map
    val simMovieMatrixBroadCast = sc.broadcast(movieRec)

    val rating = simMovieMatrixBroadCast.value.get(1339)match {
      case Some(sims) =>sims.get(217) match {
        case Some(score) =>score
        case None =>0.0
      }
      case None => 0.0
    }
    ///print(rating)

    //simMovieMatrixBroadCast.value.getOrElse(1339,0)


    // 求一个数的对数，利用换底公式，底数默认为10
    def log(m: Int): Double ={
      val N = 10
      math.log(m)/ math.log(N)
    }

    print(log(20))

    print(math.log10(20))

  }

}

