package com.ml.recommend.offlinerecommend

import com.ml.recommend.common.GlobalConstant
import com.ml.recommend.conf.SparkSess
import com.ml.recommend.domain.{MovieToUser, Recommendation, RecommendationUser, UserToMovie}
import com.ml.recommend.util.MongoUtil
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable

/**
  * @author LTong
  * @date 2019-06-18 下午 3:33
  */
object offline extends SparkSess {

  def main(args: Array[String]): Unit = {

    import spark.implicits._
    val ratingRDD = rating.rdd.map(line =>(line.getAs[Int]("uid") ,line.getAs[Int]("mid") ,line.getAs[Double]("score")))
    //训练隐语义模型
    val trainData = ratingRDD.map(line =>(line._1 ,line._2,line._3)).toDF("uid" ,"mid" ,"score")
    //als建立推荐模型，显性反馈  代表偏好程度
    val als = new ALS().setMaxIter(5).setRegParam(0.02).setImplicitPrefs(false).setNonnegative(true).setAlpha(0.02).setRank(50)
      .setUserCol("uid")
      .setItemCol("mid")
      .setRatingCol("score")


    //训练模型(显性)
    val model=als.fit(trainData)
    //冷启动处理：Spark允许用户将coldStartStrategy参数设置为“drop”,以便在包含NaN值的预测的DataFrame中删除任何行
    model.setColdStartStrategy("drop")

    //为每个用户提供10大电影排名
    val movie:DataFrame= model.recommendForAllUsers(50)

    val userRecs = movie.rdd.map((line:Row)=> {
      val recommList: Seq[(Int ,Float)] = line.getAs[Seq[Row]](1).map(x =>{
        (x.getInt(0),x.getFloat(1))
      })
      UserToMovie(line.getInt(0) ,recommList.toList.map(x =>Recommendation(x._1 ,x._2)))
    }).toDF()
    MongoUtil.insertDFInMongoDB(userRecs,GlobalConstant.USER_TO_MOVIE)

    //为每个电影推荐10个候选人
    val movieUser = model.recommendForAllItems(50)

    val movieRecs = movieUser.rdd.map((line:Row)=> {
      val recommList: Seq[(Int ,Float)] = line.getAs[Seq[Row]](1).map(x =>{
        (x.getInt(0),x.getFloat(1))
      })

      MovieToUser(line.getInt(0) ,recommList.toList.map(x =>RecommendationUser(x._1 ,x._2)))
    }).toDF()

    MongoUtil.insertDFInMongoDB(movieRecs,GlobalConstant.MOVIE_TO_USER)



  }


}
