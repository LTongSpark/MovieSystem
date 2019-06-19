package com.ml.recommend.repository

import org.apache.kafka.common.serialization.StringDeserializer

/**
  * @author LTong
  * @date 2019-06-18 下午 1:01
  */
object ViolationsRepository {

  val mongoConfig:Map[String ,String] = Map(
    "mongo.uri" -> "mongodb://localhost:27017/recommender",
    "mongo.db" -> "recommender"

  )

  val esConfig:Map[String,String] = Map(
    "es.httpHosts" -> "s112:9200",
    "es.index" -> "recommender",
    "es.cluster.name" -> "my-application"
  )
  val kafkaConfig = Map(
    "kafka.topic" -> "recommender"
  )
  val kafkaParam = Map(
    "bootstrap.servers" -> "s223:9092,s224:9092,s225:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "recommender",
    "auto.offset.reset" -> "latest"
  )

  val jedisConfig =Map(
    "jedis.host" ->"localhost"
  )

}
