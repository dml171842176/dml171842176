package com.shujia.car

import com.google.gson.Gson
import com.shujia.util.CarUtil.Cars
import com.shujia.util.{CarUtil, SparkTool}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
  * 稽查布控  -- 动态修改广播变量
  *
  */
object TourismOfficialsMonitor extends SparkTool {
  /**
    * 在子类中实现run方法，实现自定义的代码逻辑
    *
    * 在子类run方法的前面加上下面两行代码
    * import spark.implicits._
    * import org.apache.spark.sql.functions._
    *
    */

  val groupId = "TourismOfficialsMonitor"

  val dcsProfix = "dcs"

  val dcsCarsKey = "dcs_cars"

  override def run(spark: SparkSession): Unit = {


    /**
      * 通过spark streaming实时读取kafka中的数据进行过滤，将布控列表中车辆过滤出来，将结果保存到redis
      *
      */


    //读取数据

    val carsDS: DStream[CarUtil.Cars] = CarUtil.createCarDStream(ssc, groupId, Constants.CAR_TOPIC)


    carsDS.foreachRDD(rdd => {
      /**
        * 创建redis链接获取布控列表
        *
        */
      val jedis = new Jedis(Constants.REDIS_HOST, Constants.REDIS_PORT)

      import scala.collection.JavaConversions._
      //查询布控列表
      val dcsCars: List[String] = jedis.smembers(dcsCarsKey).toList


      //将布控列表广播
      val broad: Broadcast[List[String]] = sc.broadcast(dcsCars)

      jedis.close()


      //取出布控列表中的车牌号
      val filterRDD: RDD[Cars] = rdd.filter(car => {
        broad.value.contains(car.car)
      })


      //将数据包到redis中
      filterRDD.foreachPartition(iter => {
        val jedis = new Jedis(Constants.REDIS_HOST, Constants.REDIS_PORT)

        //将同一个车辆的数据保存到list中
        iter.foreach(car => {
          val carId: String = car.car

          val key: String = dcsProfix + Constants.REDIS_KEY_SPLIT + carId
          val gson = new Gson()
          val jsonStr: String = gson.toJson(car)

          jedis.lpush(key, jsonStr)

        })

        jedis.close()
      })


      //清除广播变量
      broad.unpersist()

    })

  }


}
