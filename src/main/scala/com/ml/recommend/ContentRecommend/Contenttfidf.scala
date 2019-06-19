package com.ml.recommend.ContentRecommend

import com.ml.recommend.common.GlobalConstant
import com.ml.recommend.conf.SparkSess
import com.ml.recommend.domain.{MovieRecs, Recommendation}
import com.ml.recommend.util.MongoUtil
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.jblas.DoubleMatrix

/**
  * @author LTong
  * @date 2019-06-19 下午 2:13
  */
object Contenttfidf extends SparkSess{
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val movieTagsDF = movie.rdd.map(x =>(x.getAs[Int]("mid") ,x.getAs[String]("name") ,
      x.getAs[String]("genres").map(c=> if(c=='|') ' ' else c)))
      .toDF("mid", "name", "genres").cache()

    // 核心部分： 用TF-IDF从内容信息中提取电影特征向量

    //创建一个分词器  默认按照空格分词
    val tokenizer = new Tokenizer().setInputCol("genres").setOutputCol("words")
    // 用分词器对原始数据做转换，生成新的一列words
    val wordsData = tokenizer.transform(movieTagsDF)

    // 引入HashingTF工具，可以把一个词语序列转化成对应的词频
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(50)
    val featurizedData = hashingTF.transform(wordsData)

    // 引入IDF工具，可以得到idf模型
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    // 训练idf模型，得到每个词的逆文档频率
    val idfModel = idf.fit(featurizedData)
    // 用模型对原数据进行处理，得到文档中每个词的tf-idf，作为新的特征向量
    val rescaledData = idfModel.transform(featurizedData)

    val movieFeatures = rescaledData.map(
      row => ( row.getAs[Int]("mid"), row.getAs[SparseVector]("features").toArray )
    )
      .rdd
      .map(
        x => ( x._1, new DoubleMatrix(x._2) )
      )

    // 对所有电影两两计算它们的相似度，先做笛卡尔积
    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter{
        // 把自己跟自己的配对过滤掉
        case (a, b) => a._1 != b._1
      }
      .map{
        case (a, b) => {
          val simScore = consinSim(a._2, b._2)
          ( a._1, ( b._1, simScore ) )
        }
      }
      .filter(_._2._2 > 0.6)    // 过滤出相似度大于0.6的
      .groupByKey()
      .map{
        case (mid, items) => MovieRecs( mid, items.toList.sortBy(-_._2).map(x => Recommendation(x._1, x._2)) )
      }
      .toDF()
    //将数据保存在mongodb中
    MongoUtil.insertDFInMongoDB(movieRecs ,GlobalConstant.CONTENT_MOVIE_RECS)

  }
  // 求向量余弦相似度
  def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix):Double ={
    movie1.dot(movie2) / ( movie1.norm2() * movie2.norm2() )
  }

}
