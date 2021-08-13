package com.shujia.card

import com.shujia.util.HbaseUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CardDataToHbase {
  def main(args: Array[String]): Unit = {
    /**
      * 使用spark 将卡口的数据导入hbase
      *
      * 需要先在hbase中创建表
      *
      * create 'cards','info'
      *
      */

    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("CardDataToHbase")


    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("data/cards.csv")

    //将数据保存到hbase中
    rdd.foreachPartition(iter => {

      val connection: Connection = HbaseUtil.getConnection

      //获取一个表的对象
      val table: Table = connection.getTable(TableName.valueOf("cards"))

      iter.foreach(card => {
        val split: Array[String] = card.split(",")
        val cardId: String = split(0)
        val lon: String = split(1)
        val lat: String = split(2)

        //构建put 指定rowkey
        val put = new Put(cardId.getBytes())

        //增加列
        put.addColumn("info".getBytes(), "lon".getBytes(), lon.getBytes)
        put.addColumn("info".getBytes(), "lat".getBytes(), lat.getBytes)

        //插入数据
        table.put(put)

      })

      connection.close()

    })

  }
}
