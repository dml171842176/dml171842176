package com.shujia.car

import java.text.SimpleDateFormat
import java.util.Date

import com.shujia.util.{CarUtil, SparkTool}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
  *
  * 实时统计每个卡扣当天总流量
  */
object SumCardOnDayFlux extends SparkTool {
  /**
    * 在子类中实现run方法，实现自定义的代码逻辑
    *
    * 在子类run方法的前面加上下面两行代码
    * import spark.implicits._
    * import org.apache.spark.sql.functions._
    *
    */

  //消费者组
  val groupId = "SumCardOnDayFlux"
  //数据保存在redis中前缀
  val keyPrefix = "SumCardOnDayFlux"
  //时间格式化
  val datePattern = "yyyy-MM-dd"

  override def run(spark: SparkSession): Unit = {

    //读取数据
    val carsDS: DStream[CarUtil.Cars] = CarUtil.createCarDStream(ssc, groupId, Constants.CAR_TOPIC)


    /**
      *
      * 实时统计每个卡扣当天总流量
      */

    /**
      * 1、将每隔batch的统计捷豹保存到redis中
      * 2、下一次统计的时候基于redis中的结果进行累加
      * 3、判断当前时间，如果是0点将redis中的数据清空
      *
      */

    carsDS.foreachRDD(rdd => {


      //统计当前batch卡口的车流量
      val currBatchFlux: RDD[(String, Int)] = rdd
        .map(car => {
          //卡口编号
          val card: Long = car.card

          val time: Long = car.time
          val date = new Date(time * 1000)
          val format = new SimpleDateFormat(datePattern)
          //获取事件时间的天
          val day: String = format.format(date)

          val key: String = day + ":" + card

          (key, 1)
        })

        .reduceByKey(_ + _)


      //将数据累加保存到redis中
      currBatchFlux.foreachPartition(iter => {

        val jedis = new Jedis(Constants.REDIS_HOST, Constants.REDIS_PORT)

        iter.foreach(kv => {
          val cardAndDay: String = kv._1
          val count: Int = kv._2

          val key: String = keyPrefix + Constants.REDIS_KEY_SPLIT + cardAndDay

          //累加计算
          jedis.incrBy(key, count)


          //设置一个过期时间，自动删除数据
          jedis.expire(key, 24 * 60 * 60)
        })

        jedis.close()
      })

    })


  }
}
