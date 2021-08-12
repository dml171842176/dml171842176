package com.shujia.car

import java.util

import com.google.gson.Gson
import com.shujia.util.SparkTool
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Duration, Durations, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * 稽查布控
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
    * @param spark spark的环境
    */
  override def run(spark: SparkSession): Unit = {

    /**
      * 构建Spark Stream环境
      *
      */

    val sc: SparkContext = spark.sparkContext

    val ssc = new StreamingContext(sc, Durations.seconds(5))


    /**
      * 读取kafka中的数据
      *
      */
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "master:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "asdasd",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> "false"
    )

    //topic 列表
    val topics = Array("cars")

    /**
      * createDirectStream: 主动拉取数据
      */

    val carsRecordDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    /**
      * 通过spark streaming实时读取kafka中的数据进行过滤，将布控列表中车辆过滤出来，将结果保存到redis
      *
      */


    val carsDS: DStream[String] = carsRecordDS.map(_.value())

    val carsCaseDS: DStream[Cars] = carsDS.map(line => {
      val gson = new Gson()
      val cars: Cars = gson.fromJson(line, classOf[Cars])
      cars
    })

    carsCaseDS.foreachRDD(rdd => {
      /**
        * 创建redis链接获取布控列表
        *
        */
      val jedis = new Jedis("master", 6379)

      import scala.collection.JavaConversions._
      //查询布控列表
      val dcsCars: List[String] = jedis.smembers("dcs_cars").toList


      jedis.close()


      //取出布控列表中的车牌号
      val filterRDD: RDD[Cars] = rdd.filter(car => {
        dcsCars.contains(car.car)
      })


      //将数据包到redis中
      filterRDD.foreachPartition(iter => {
        val jedis = new Jedis("master", 6379)

        //将同一个车辆的数据保存到list中
        iter.foreach(car => {
          val carId: String = car.car

          val key: String = "dcs:" + carId
          val gson = new Gson()
          val jsonStr: String = gson.toJson(car)

          jedis.lpush(key, jsonStr)

        })

        jedis.close()
      })

    })


    ssc.start()
    ssc.awaitTermination()
    ssc.stop()


  }

  case class Cars(car: String, city_code: String, county_code: String, card: Long, camera_id: String, orientation: String, road_id: Long, time: Long, speed: Double)

}
