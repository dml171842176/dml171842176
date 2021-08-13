package com.shujia.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

object HbaseUtil {
  def getConnection: Connection = {
    //1、创建hbase的链接
    val configuration: Configuration = new Configuration()

    //指定zk的链接地址
    configuration.set("hbase.zookeeper.quorum", "master")

    val connection: Connection = ConnectionFactory.createConnection(configuration)

    connection
  }
}
