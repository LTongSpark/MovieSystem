package com.ml.recommend.util

import java.net.InetAddress

import com.ml.recommend.domain.ESConfig
import org.apache.spark.sql.DataFrame
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

/**
  * @author LTong
  * @date 2019-06-18 下午 12:53
  */
object ESUtil {

  /**
    * 保存数据到es
    *
    */
  def storeDataInES(movieWithTagsDF: DataFrame ,esIndex:String)(implicit eSConfig: ESConfig): Unit = {

    //指定es集群
    val settings:Settings = Settings.builder().put("cluster.name","my-application")
      .put("client.transport.sniff", true)
      .build()

    //创建访问es服务器的客户端

    val client:TransportClient = new PreBuiltTransportClient(settings)
      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("s112") ,9300))

    // 先清理遗留的数据
    if( client.admin().indices().exists( new IndicesExistsRequest(eSConfig.index) )
      .actionGet()
      .isExists
    ){
      client.admin().indices().delete( new DeleteIndexRequest(eSConfig.index) )
    }
    client.admin().indices().create( new CreateIndexRequest(eSConfig.index) )

    movieWithTagsDF.write
      .option("es.nodes", eSConfig.httpHosts)
      .option("es.http.timeout", "100m")
      .option("es.mapping.id", "mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(eSConfig.index + "/" + esIndex)
  }

}
