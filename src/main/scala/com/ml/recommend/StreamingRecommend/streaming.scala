package com.ml.recommend.StreamingRecommend

import com.ml.recommend.common.GlobalConstant
import com.ml.recommend.conf.SparkSess
import com.ml.recommend.domain.MongoConfig
import com.ml.recommend.repository.ViolationsRepository
import com.ml.recommend.util.{JPools, MongoUtil}
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import redis.clients.jedis.Jedis

/**
  * @author LTong
  * @date 2019-06-18 下午 11:23
  */
object streaming extends SparkSess{
  def main(args: Array[String]): Unit = {
    // 加载电影相似度矩阵数据，把它广播出去
    val movieRec: collection.Map[Int,Map[Int ,Double]] = MongoUtil.loadMovieRecDFInMongoDB(spark, GlobalConstant.MOVIE_RECS)
    //问了方便查询  转换成map
    val simMovieMatrixBroadCast = sc.broadcast(movieRec)

    //从redis中获取偏移量
    val offsetMap = Map[TopicPartition, Long]()

    //链接到kafka的数据源
    val kafkaStream:InputDStream[ConsumerRecord[String, String]] = if(offsetMap.size == 0){
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Array(ViolationsRepository.kafkaConfig("kafka.topic")), ViolationsRepository.kafkaParam))
    }else{
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Assign[String, String](offsetMap.keys ,ViolationsRepository.kafkaParam,offsetMap))
    }

    // 把原始数据UID|MID|SCORE|TIMESTAMP 转换成评分流
    val ratingStream = kafkaStream.map{
      msg =>
        val attr = msg.value().split("\\|")
        ( attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt )
    }
      ratingStream.foreachRDD(rdds =>{
        //判断rdd非空
        if(!rdds.isEmpty()){
          val offsetRange: Array[OffsetRange] = rdds.asInstanceOf[HasOffsetRanges].offsetRanges
          rdds.foreach(rdd => {
            println("rating data coming! >>>>>>>>>>>>>>>>")
            // 1. 从redis里获取当前用户最近的K次评分，保存成Array[(mid, score)]  uid, mid, score, timestamp
            val userRecentlyRatings = getUserRecentlyRating( GlobalConstant.MAX_USER_RATINGS_NUM, rdd._1, JPools.getJedis)

            // 2. 从相似度矩阵中取出当前电影最相似的N个电影，作为备选列表，Array[mid]
            val candidateMovies = getTopSimMovies(GlobalConstant.MAX_SIM_MOVIES_NUM, rdd._2, rdd._1, simMovieMatrixBroadCast.value)

            // 3. 对每个备选电影，计算推荐优先级，得到当前用户的实时推荐列表，Array[(mid, score)]
            val streamRecs = computeMovieScores(candidateMovies, userRecentlyRatings, simMovieMatrixBroadCast.value)
            //将推荐数据保存在mongodb
            MongoUtil.saveDataToMongoDB(rdd._1,streamRecs)

          })
          //将偏移量存到redis
          val jedis = JPools.getJedis
          for (offset <- offsetRange) {
            //  yc-info-0
            jedis.hset(ViolationsRepository.kafkaConfig("group.id"), offset.topic + "-" + offset.partition, offset.untilOffset.toString)
          }
        }

      })
    // redis操作返回的是java类，为了用map操作需要引入转换类
    import scala.collection.JavaConversions._
    def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis): Array[(Int,Double)]= {
      //从redis读取数据，用户评分数据保存在uid：uid为key的队列里，value是score评分
      jedis.lrange("uid:" + uid ,0 ,num-1).map(rdd =>{
        val attr = rdd.split("\\:")
        (attr(0) .trim.toInt ,attr(1).trim.toDouble)
      }).toArray
    }

    /**
      * 获取跟当前电影做相似的num个电影，作为备选电影
      * @param num       相似电影的数量
      * @param mid       当前电影ID
      * @param uid       当前评分用户ID
      * @param simMovies 相似度矩阵
      * @return          过滤之后的备选电影列表
      */
    def getTopSimMovies(num:Int,mid:Int ,uid:Int,simMovies:collection.Map[Int,Map[Int,Double]])(implicit mongoConfig: MongoConfig): Array[Int] ={
      //1、从相似度矩阵中拿出所有相似的电影
      val allSimMovie = simMovies(mid).toArray

      //2、从mongodb中查询这个用户已经看过的电影

      val ratingExis =MongoClient(MongoClientURI(mongoConfig.uri))(mongoConfig.db)(GlobalConstant.MONGODB_RATING_COLLECTION)
        .find(MongoDBObject("uid" ->uid))
        .toArray
        .map(_.get("mid").toString.toInt)

      //把看过的过滤掉 得到输出结果
      allSimMovie.filter( x=> !ratingExis.contains(x._1) )
        .sortBy(-_._2)
        .take(num)
        .map(x=>x._1)
    }

    /**
      *
      * @param candidateMovies   最近的一次评分
      * @param userRecentlyRatings    最相似的电影
      * @param value
      */
    def computeMovieScores(candidateMovies: Array[Int], userRecentlyRatings: Array[(Int, Double)], value:
                            collection.Map[Int, Map[Int, Double]]):Array[(Int ,Double)] ={
      //定义一个ArrayBuffer  用于保存每一个备选电影的基础的分
      val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()

      // 定义一个HashMap，保存每一个备选电影的增强减弱因子
      val increMap = scala.collection.mutable.HashMap[Int, Int]()
      val decreMap = scala.collection.mutable.HashMap[Int, Int]()

      for(candidateMovie <- candidateMovies; userRecentlyRating <- userRecentlyRatings){
        //拿到备选电影和最近平份额电影的相似度
        val simScore:Double = simMovieMatrixBroadCast.value.getOrElse(candidateMovie,0.0).asInstanceOf[Map[Int,Double]].getOrElse(userRecentlyRating._1,0.0)

        if(simScore > 0.7){
          //计算备选电影的基础推荐得分
          scores += ((candidateMovie, simScore * userRecentlyRating._2))
          if(userRecentlyRating._2 >3){
            increMap(candidateMovie) = increMap.getOrDefault(candidateMovie,0) + 1
          }else{
            decreMap(candidateMovie) = decreMap.getOrDefault(candidateMovie, 0) + 1
          }
        }
      }

      //根据备选电影的mid做groupby 根据公式求最后的推荐评分
      scores.groupBy(_._1).map{
        // groupBy之后得到的数据 Map( mid -> ArrayBuffer[(mid, score)] )
        case(mid,scoreList) =>(mid ,scoreList.map(_._2).sum /scoreList.length +
          math.log10(increMap.getOrDefault(mid, 1) - math.log10(decreMap.getOrDefault(mid, 1))))
      }.toArray.sortBy(-_._2)
    }





  }

}
