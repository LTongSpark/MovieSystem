package com.ml.recommend.dataloader

import com.ml.recommend.domain._
import com.ml.recommend.util.{ESUtil, MongoUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.ml.recommend.common.GlobalConstant
import com.ml.recommend.conf.SparkSess
import com.ml.recommend.repository.ViolationsRepository

/**
  * @author LTong
  * @date 2019-06-17 下午 4:13
  */

object DataLoader extends SparkSess {
  //主函数
  def main(args: Array[String]): Unit = {

    val movieRDD: RDD[String] = spark.sparkContext.textFile(GlobalConstant.MOVIE_DATA_PATH)
    //加载数据
    import spark.implicits._
    val movieDF:DataFrame = movieRDD.map(item =>{
      val attr = item.split("\\^")
      Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim,
        attr(4).trim, attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
    }).toDF()

    val ratingRDD:RDD[String] = spark.sparkContext.textFile(GlobalConstant.RATING_DATA_PATH)
    val ratingDF:DataFrame = ratingRDD.map(rating =>{
      val attr = rating.split(",")
      Rating(attr(0).toInt ,attr(1).toInt ,attr(2).toDouble ,attr(3).toInt)
    }).toDF()

    val tagRDD:RDD[String] = spark.sparkContext.textFile(GlobalConstant.TAG_DATA_PATH)
    val tagDF:DataFrame = tagRDD.map(tag =>{
      val attr: Array[String] = tag.split(",")
      Tag(attr(0).toInt,attr(1).toInt,attr(2).trim,attr(3).toInt)
    }).toDF()

     implicit val mongoConfig = MongoConfig(ViolationsRepository.mongoConfig("mongo.uri") ,ViolationsRepository.mongoConfig("mongo.db"))

    //将数据保存在mongodb中

   MongoUtil.storeDataInMongoDB(movieDF, ratingDF, tagDF)

    //数据预处理  把movie对应的tag信息添加进去，加一列 tag1|tag2|tag3...  标签
    import org.apache.spark.sql.functions._

    /**
      * |1088  |80's classic|music|girlie movie|rich families|dance|musical parodies                                                  |
      * |1721  |romance                                                                                                               |
      * |27255 |Views|No progress|Too slow
      */
    val newTag :DataFrame= tagDF.groupBy("mid")
      .agg(concat_ws("|" ,collect_set("tag")).as("tags"))
      .select("mid" ,"tags")
    movieDF.show(false)

    // newTag和movie做join，数据合并在一起，左外连接
    val movieWithTagsDF:DataFrame = movieDF.join(newTag, Seq("mid"), "left")

    movieWithTagsDF.show(false)

    implicit val esConfig = ESConfig(ViolationsRepository.esConfig("es.httpHosts"),
      ViolationsRepository.esConfig("es.index"), ViolationsRepository.esConfig("es.cluster.name"))

    // 保存数据到ES
    ESUtil.storeDataInES(movieWithTagsDF,GlobalConstant.ES_MOVIE_INDEX)
  }




}
