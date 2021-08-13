package com.shujia.car

import com.shujia.util.Config

object Constants {

  // 过车数据的topic
  val CAR_TOPIC: String = Config.get("car.topic")

  //checkpoint路径
  val CHECKPOINT_DIR: String = Config.get("checkpoint.dir")

  //redis的主机名
  val REDIS_HOST: String = Config.get("redis.host")
  //redis的端口号
  val REDIS_PORT: Int = Config.get("redis.port").toInt

  //数据保存在redis中，key 的分隔符
  val REDIS_KEY_SPLIT: String = Config.get("redis.key.split")

  //卡口数据在hbase中的表名
  val HBASE_CARD_TABLE_NAME: String = Config.get("hbase.card.table.name")

}
