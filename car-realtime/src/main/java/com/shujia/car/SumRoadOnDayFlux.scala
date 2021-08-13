package com.shujia.car

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.shujia.util.{CarUtil, SparkTool}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
  * 2.1 实时统计每个道路当天总流量
  *
  */
object SumRoadOnDayFlux extends SparkTool {

  //时间格式化
  val datePattern = "yyyy-MM-dd"
  val SumRoadOnDayFluxCars = "SumRoadOnDayFluxCars"

  val SumRoadOnDayFlux = "SumRoadOnDayFlux"

  var groupId = "SumRoadOnDayFlux"

  /**
    * 在子类中实现run方法，实现自定义的代码逻辑
    *
    * 在子类run方法的前面加上下面两行代码
    * import spark.implicits._
    * import org.apache.spark.sql.functions._
    *
    * @param spark spark的环境
    */
  override def run(spark: SparkSession): Unit = {
    //读取数据
    val carDS: DStream[CarUtil.Cars] = CarUtil.createCarDStream(ssc, groupId, Constants.CAR_TOPIC)


    carDS.foreachRDD(rdd => {

      //取出每个道路的车辆，去重
      val distinctRDD: RDD[(Long, String, String)] = rdd.map(car => {
        val roadId: Long = car.road_id
        val carId: String = car.car
        val time: Long = car.time
        val date = new Date(time * 1000)
        val format = new SimpleDateFormat(datePattern)
        val day: String = format.format(date)

        (roadId, day, carId)
      }).distinct()

      //按照道路和时间分组
      val groupByRDD: RDD[((Long, String), Iterable[String])] = distinctRDD
        .map(kv => ((kv._1, kv._2), kv._3))
        .groupByKey()

      groupByRDD.foreachPartition(iter => {

        //创建链接
        val jedis = new Jedis(Constants.REDIS_HOST, Constants.REDIS_PORT)


        iter.foreach {
          case ((roadId: Long, day: String), carIds: Iterable[String]) =>
            //1、将道路的车辆保存到redis的 set集合中
            val key: String = SumRoadOnDayFluxCars + Constants.REDIS_KEY_SPLIT + day + Constants.REDIS_KEY_SPLIT + roadId

            //循环将车辆保存到redis中
            carIds.foreach(id => jedis.sadd(key, id))

            //2、计算当前道路在redis集合中保存车辆的数量
            val count: lang.Long = jedis.scard(key)



            //3、将车流量保存到redis中

            val key1: String = SumRoadOnDayFlux + Constants.REDIS_KEY_SPLIT + day + Constants.REDIS_KEY_SPLIT + roadId
            jedis.set(key1, count.toString)

            //设置一个过期时间，自动删除数据
            jedis.expire(key, 24 * 60 * 60)
            jedis.expire(key1, 24 * 60 * 60)
        }

        jedis.close()
      })
    })
  }
}
