package com.ml.recommend.offlinerecommend

import com.ml.recommend.conf.SparkSess
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.DataFrame

/**
  * @author LTong
  * @date 2019-06-18 下午 10:10
  */
object ALSTrainer extends SparkSess {
  def main(args: Array[String]): Unit = {
   import spark.implicits._
    // 加载数据
    val ratingRDD = rating.rdd.map(line =>(line.getAs[Int]("uid") ,line.getAs[Int]("mid") ,line.getAs[Double]("score")))
    //提取uid和mid并去重
    val userRDD = ratingRDD.map(_._1).distinct()
    val movieRDD = ratingRDD.map(_._2).distinct()

    //训练隐语义模型
    val train = ratingRDD.map(line =>(line._1 ,line._2,line._3)).toDF("uid" ,"mid" ,"score")

    //分割数据
    val Array(trainData ,testData) = train.randomSplit(Array(0.8,0.2))

    // 模型参数选择，输出最优参数
    adjustALSParam(trainData, testData)
  }

  def adjustALSParam(trainData: DataFrame, testData: DataFrame): Unit ={
    val result = for( rank <- Array(50, 100); alpha <- Array( 0.01, 0.02))
      yield {
        val als = new ALS().setMaxIter(5)
          .setImplicitPrefs(false)
          .setNonnegative(true)
          .setAlpha(alpha)
          .setRank(rank)
          .setUserCol("uid")
          .setItemCol("mid")
          .setRatingCol("score")

        //训练模型(显性)
        val model=als.fit(trainData)
        //冷启动处理：Spark允许用户将coldStartStrategy参数设置为“drop”,以便在包含NaN值的预测的DataFrame中删除任何行
        model.setColdStartStrategy("drop")

        //测试数据
        val prediction=model.transform(testData)

        //模型评估
        val evaluator = new RegressionEvaluator().setMetricName("rmse")
          .setLabelCol("score")
          .setPredictionCol("prediction")

        // 计算当前参数对应模型的rmse，返回Double
        val rmse=evaluator.evaluate(prediction)

        ( rank, alpha, rmse)
      }
    // 控制台打印输出最优参数
    println(result.sortBy(_._3).head)
  }

}
