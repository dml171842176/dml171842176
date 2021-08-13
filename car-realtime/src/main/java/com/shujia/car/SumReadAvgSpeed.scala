package com.shujia.car

import java.text.SimpleDateFormat
import java.util.Date

import com.shujia.util.{CarUtil, SparkTool}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Durations, Time}
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
  * 实时统计道路拥堵情况-  实时计算每个道路平均速度
  *
  */
object SumReadAvgSpeed extends SparkTool {

  val groupId = "SumReadAvgSpeed"
  val datePattern = "yyyy-MM-dd:HH:mm:ss"
  val SumReadAvgSpeed = "SumReadAvgSpeed"

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

    val carsDS: DStream[CarUtil.Cars] = CarUtil.createCarDStream(ssc, groupId, Constants.CAR_TOPIC)

    /**
      * 计算最近10分钟道路车辆的数量，以及平均车速，每个10秒计算一次
      *
      */

    val roadCarNumAndSumSpeed: DStream[(Long, (Double, Int))] = carsDS.map(car => {
      val road_id: Long = car.road_id
      val speed: Double = car.speed

      (road_id, (speed, 1))
    }).reduceByKeyAndWindow(
      //总车速和总车辆一起计算
      (x: (Double, Int), y: (Double, Int)) => (x._1 + y._1, x._2 + y._2),
      Durations.minutes(10),
      Durations.seconds(10)
    )

    //计算平均车速
    val roadCarNumAndAvgSpeed: DStream[(Long, Double, Int)] = roadCarNumAndSumSpeed.map {
      case (road: Long, (sumSpeed: Double, sumCar: Int)) =>
        val avgSpeed: Double = sumSpeed / sumCar

        (road, avgSpeed, sumCar)
    }

    //将数据保存到redis中

    roadCarNumAndAvgSpeed.foreachRDD((rdd: RDD[(Long, Double, Int)], time: Time) => {


      //获取当前计算的时间
      val ts: Long = time.milliseconds
      val date = new Date(ts)
      val format = new SimpleDateFormat(datePattern)
      val day: String = format.format(date)


      rdd.foreachPartition(iter => {

        //创建redis链接
        val jedis = new Jedis(Constants.REDIS_HOST, Constants.REDIS_PORT)


        iter.foreach {
          case (road: Long, avgSpeed: Double, sumCar: Int) =>
            val key: String = SumReadAvgSpeed + Constants.REDIS_KEY_SPLIT + day + Constants.REDIS_KEY_SPLIT + road

            //使用map 结构保存数据
            jedis.hset(key, "avgSpeed", avgSpeed.toString)
            jedis.hset(key, "sumCar", sumCar.toString)


            jedis.expire(key, 24 * 60 * 60)
        }

        jedis.close()
      })

    })


  }
}
