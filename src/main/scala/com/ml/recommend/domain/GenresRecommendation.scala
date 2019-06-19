package com.ml.recommend.domain

/**
  * @author LTong
  * @date 2019-06-18 上午 11:49
  *   定义电影类别top10推荐对象
  */

case class GenresRecommendation( genres: String, recs: Seq[Recommendation])
