package com.ml.recommend.domain

/**
  * @author LTong
  * @date 2019-06-18 下午 3:35
  *      定义基于LFM电影特征向量的电影相似度列表
  */
case class MovieRecs(mid: Int, recs: Seq[Recommendation])
