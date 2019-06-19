package com.ml.recommend.util

/**
  * @author LTong
  * @date 2019-06-19 上午 11:45
  */
object other {
  //获得两个电影的相似度
  def getMoviesSimScore(mid1: Int, mid2: Int, simMovies: collection.Map[Int, Map[Int, Double]]): Double ={

    simMovies.get(mid1) match {
      case Some(sims) => sims.get(mid2) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

}
