package com.shujia.car

import com.shujia.util.{CarUtil, HbaseUtil, SparkTool}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Get, Result, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.DStream

/**
  * 将过车数据保存到es 中
  *
  */
object CarDataToES extends SparkTool {

  val groupId = "CarDataToES"

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
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //读取数据
    val carsDS: DStream[CarUtil.Cars] = CarUtil.createCarDStream(ssc, groupId, Constants.CAR_TOPIC)


    carsDS.foreachRDD(rdd => {


      val joinRDD: RDD[(String, String, Long, Double, Long, Long, String, String, String, Double, Double)] = rdd.mapPartitions(iter => {

        //获取hbase 链接
        val connection: Connection = HbaseUtil.getConnection

        //获取一个表的对象
        val table: Table = connection.getTable(TableName.valueOf(Constants.HBASE_CARD_TABLE_NAME))

        iter.map(car => {
          val cardId: Long = car.card

          //通过卡口编号查看卡口的经纬度
          val get = new Get(cardId.toString.getBytes)
          val result: Result = table.get(get)

          val lon: Double = Bytes.toString(result.getValue("info".getBytes(), "lon".getBytes())).toDouble
          val lat: Double = Bytes.toString(result.getValue("info".getBytes(), "lat".getBytes())).toDouble

          (car.car,
            car.camera_id,
            car.time,
            car.speed,
            car.road_id,
            car.card,
            car.orientation,
            car.county_code,
            car.city_code,
            lon,
            lat
          )
        })
      })

      //将数据保存到es 中
      val carDS: DataFrame = joinRDD.toDF(
        "car",
        "camera_id",
        "time",
        "speed",
        "road_id",
        "card",
        "orientation",
        "county_code",
        "city_code",
        "lon",
        "lat"
      )

      //增加唯一字段
      val resultDS: DataFrame = carDS.withColumn("id", concat($"car", expr("'_'"), $"time"))
        .withColumn("location", array($"lat", $"lon"))


      import org.elasticsearch.spark.sql._
      val options = Map(
        "es.index.auto.create" -> "true",
        "es.nodes.wan.only" -> "true",
        "es.nodes" -> "master",
        "es.port" -> "9200",
        "es.mapping.id" -> "id",
        "es.batch.write.retry.count" -> "10",
        "es.batch.write.retry.wait" -> "60",
        "es.http.timeout" -> "100s"

      )


      /*

PUT /cars/
{
  "mappings": {
      "properties":{
        "location":{
          "type":"geo_point"
        }
    }
  }
}

       */

      //将数据保存到中
      resultDS.saveToEs("cars", options)

    })


  }
}
