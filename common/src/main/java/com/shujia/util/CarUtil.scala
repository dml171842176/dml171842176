package com.shujia.util

import com.google.gson.Gson
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object CarUtil {

  /**
    * 读取kafka中数据的工具
    *
    * @param ssc     ： spark 环境
    * @param groupId 消费者组
    * @param topic   ： topic
    * @return : ds
    */
  def createCarDStream(ssc: StreamingContext, groupId: String, topic: String*): DStream[Cars] = {

    /**
      * 读取kafka中的数据
      */
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "master:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> "false"
    )

    /**
      * createDirectStream: 主动拉取数据
      */

    val carsRecordDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topic, kafkaParams)
    )



    val carsDS: DStream[String] = carsRecordDS.map(_.value())

    val carsCaseDS: DStream[Cars] = carsDS.map(line => {
      val gson = new Gson()
      val cars: Cars = gson.fromJson(line, classOf[Cars])
      cars
    })

    //返回ds
    carsCaseDS

  }

  case class Cars(car: String, city_code: String, county_code: String, card: Long, camera_id: String, orientation: String, road_id: Long, time: Long, speed: Double)


}
