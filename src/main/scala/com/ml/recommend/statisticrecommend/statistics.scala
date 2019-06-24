package com.ml.recommend.statisticrecommend

import com.ml.recommend.common.GlobalConstant
import com.ml.recommend.conf.SparkSess
import com.ml.recommend.domain._
import com.ml.recommend.util.{DateUtil, MongoUtil}
import org.apache.spark.sql.functions._

/**
  * @author LTong
  * @date 2019-06-18 上午 11:48
  */
object statistics extends SparkSess {
  def main(args: Array[String]): Unit = {

    import spark.implicits._
    /**
      * 不同的推荐结果
      */
      //1、历史热门统计，历史评分数据最多
    val rateMoreMoviesDF = rating.groupBy(rating.col("mid"))
        .agg(count(rating.col("mid")).as("count"))
      .select("mid" ,"count")
    MongoUtil.insertDFInMongoDB(rateMoreMoviesDF,GlobalConstant.RATE_MORE_MOVIES)

    //对原始数据做预处理 去掉uid
    //2. 近期热门统计，按照“yyyyMM”格式选取最近的评分数据，统计评分个数
    val ratingOfYearMonth = rating.select(rating.col("mid") ,rating.col("score"),
      rating.col("timestamp")).rdd.map(data =>{
      (data.getAs[Int]("mid") , data.getAs[Double]("score"),
        DateUtil.parse(data.getAs[Int]("timestamp")))
    }).toDF("mid","score","yearmonth")

    val rateMoreRecentlyMoviesDF = ratingOfYearMonth.groupBy("yearmonth","mid")
      .agg(count(ratingOfYearMonth.col("mid")).as("count"))
      .orderBy(ratingOfYearMonth.col("yearmonth").desc,$"count".desc)
      .select("mid" ,"count" ,"yearmonth")

    // 存入mongodb
    MongoUtil.insertDFInMongoDB(rateMoreRecentlyMoviesDF, GlobalConstant.RATE_MORE_RECENTLY_MOVIES)


    // 3. 优质电影统计，统计电影的平均评分，mid，avg
    val averageMoviesDF = rating.groupBy(rating.col("mid"))
      .agg(avg(rating.col("score")).as("avg"))
        .select("mid" ,"avg")
    // 存入mongodb
    MongoUtil.insertDFInMongoDB(rateMoreRecentlyMoviesDF, GlobalConstant.AVERAGE_MOVIES)

    // 4. 各类别电影Top统计
    // 定义所有类别
    val genres = List("Action","Adventure","Animation","Comedy","Crime","Documentary","Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery"
      ,"Romance","Science","Tv","Thriller","War","Western")

    // 把平均评分加入movie表里，加一列，inner join
    val movieWithScore = movie.join(averageMoviesDF, "mid")
    movieWithScore.show(false)

    // 为做笛卡尔积，把genres转成rdd
    val genresRDD = spark.sparkContext.makeRDD(genres)
    //计算类别top10 首先对类别和电影做笛卡尔积
    val genresTopMoviesDF = genresRDD.cartesian(movieWithScore.rdd)
      .filter{
        //条件过滤  找出movie的字段genres值(Action|Adventure|Sci-Fi)包含当前类别genre(Action)的那些
        case(genre,movieRow) =>movieRow.getAs[String]("genres").toLowerCase.contains(genre.toLowerCase)
      }
        .map{
          case(genre,movieRow) =>(genre ,(movieRow.getAs[Int]("mid"), movieRow.getAs[Double]("avg")))
        }
        .groupByKey()
        .map{
          case (genre, items) => GenresRecommendation( genre, items.toList.sortBy(-_._2)
            .take(10)
            .map( item=> Recommendation(item._1, item._2)))
        }.toDF()

    MongoUtil.insertDFInMongoDB(genresTopMoviesDF, GlobalConstant.GENRES_TOP_MOVIES)
  }

}
