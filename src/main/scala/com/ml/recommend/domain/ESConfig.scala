package com.ml.recommend.domain

/**
  * @author LTong
  * @date 2019-06-17 下午 4:49
  * es配置
  * httpHosts http主机名，逗号分隔
  * transportHosts  主机列表
  * index 操作的索引
  * clustername 集群名称
  *
  */
case class ESConfig(httpHosts:String,index:String, clustername:String)
