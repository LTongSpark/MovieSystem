package com.ml.recommend.util

import com.ml.recommend.repository.ViolationsRepository
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * @author LTong
  * @date 19-6-12 13:16
  *      redis链接池
  */
object JPools {
  private  val  poolConf = new GenericObjectPoolConfig()

  poolConf.setMaxIdle(5) //最大空闲链接
  poolConf.setMaxTotal(2000)

  //链接池
  private lazy val jedisPool = new JedisPool(poolConf ,ViolationsRepository.jedisConfig("jedis.host"))
  def getJedis = {
    val jedis = jedisPool.getResource
    jedis.select(3)
    jedis
  }
}
