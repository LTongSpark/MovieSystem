package com.ml.recommend.common

/**
  * @author LTong
  * @date 2019-06-18 上午 11:50
  */
object GlobalConstant {

  /**
    * 数据目录
    */
  val MOVIE_DATA_PATH:String = "D:\\idea\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH:String = "D:\\idea\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH:String = "D:\\idea\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\tags.csv"
  /**
    * mongdb 相关
    */
  val MONGODB_MOVIE_COLLECTION:String = "movie"
  val MONGODB_RATING_COLLECTION:String = "rating"
  val MONGODB_TAG_COLLECTION:String = "tag"
  val ES_MOVIE_INDEX:String = "movie"

  //统计的表的名称
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"
  val USER_TO_MOVIE = "UserToMovie"
  val MOVIE_TO_USER = "MovieToUser"
  val USER_MAX_RECOMMENDATION = 20
  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val CONTENT_MOVIE_RECS = "ContentMovieRecs"



}
