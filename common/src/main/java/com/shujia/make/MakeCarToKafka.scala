package com.shujia.make

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

object MakeCarToKafka {
  def main(args: Array[String]): Unit = {
    //1、创建kafka生产者

    val properties = new Properties()

    //1、指定kafkabroker地址
    properties.setProperty("bootstrap.servers", "master:9092")

    //2、指定kv的序列化类
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    //创建生产者
    val producer = new KafkaProducer[String, String](properties)


    val cars: Iterator[String] = Source.fromFile("D:\\data\\car\\cars.json").getLines()


    var i = 0
    //代码重启后将max f改成上一次的i
    val max = 27133

    for (car <- cars) {
      i += 1
      if (i > max) {
        val record = new ProducerRecord[String, String]("cars", car)

        producer.send(record)
        producer.flush()

        Thread.sleep(20)
      }

      println(i)
    }


    producer.close()

  }
}
