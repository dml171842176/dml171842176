package com.shujia.car

import java.text.SimpleDateFormat
import java.util.Date

import com.shujia.util.{CarUtil, SparkTool}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Time
import redis.clients.jedis.Jedis

/**
  *
  * 1.2 按窗口统计每个卡扣流量 - 统计每个卡扣最近5分钟车流量，每隔1分钟统计一次
  *
  */
object SumCardOnWindowFlux extends SparkTool {
  /**
    * 在子类中实现run方法，实现自定义的代码逻辑
    *
    * 在子类run方法的前面加上下面两行代码
    * import spark.implicits._
    * import org.apache.spark.sql.functions._
    *
    */

  val groupId = "SumCardOnWindowFlux"

  val datePattern = "yyyy-MM-dd:HH:mm"

  val keyPrefix = "SumCardOnWindowFlux"

  override def run(spark: SparkSession): Unit = {

    ssc.checkpoint(Constants.CHECKPOINT_DIR)

    //读取数据
    val carDS: DStream[CarUtil.Cars] = CarUtil.createCarDStream(ssc, groupId, Constants.CAR_TOPIC)


    /**
      * 1.2 按窗口统计每个卡扣流量 - 统计每个卡扣最近5分钟车流量，每隔1分钟统计一次
      *
      */

    val kvDS: DStream[(Long, Int)] = carDS.map(car => (car.card, 1))


    val countDS: DStream[(Long, Int)] = kvDS.reduceByKeyAndWindow(
      (x: Int, y: Int) => x + y,
      (x: Int, y: Int) => x - y, //开启checkpoint
      Durations.minutes(5),
      Durations.minutes(1)
    )

    countDS.foreachRDD((rdd: RDD[(Long, Int)], time: Time) => {
      //获取当前计算的时间
      val ts: Long = time.milliseconds
      val date = new Date(ts)
      val format = new SimpleDateFormat(datePattern)
      val day: String = format.format(date)


      /**
        * 将结果保存到redis中，已卡口和时间戳作为key
        *
        */

      rdd.foreachPartition(iter => {
        val jedis = new Jedis(Constants.REDIS_HOST, Constants.REDIS_PORT)

        iter.foreach(kv => {
          val card: Long = kv._1

          val count: Int = kv._2

          val key: String = keyPrefix + Constants.REDIS_KEY_SPLIT + day + Constants.REDIS_KEY_SPLIT + card

          jedis.set(key, count.toString)

        })

        jedis.close()
      })

    })


    ssc.start()
    ssc.awaitTermination()
    ssc.stop()


  }
}
