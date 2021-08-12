package com.shujia.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

abstract class SparkTool extends Logging {

  /**
    * 子类会继承父类的mainH函数
    *
    */

  var spark: SparkSession = _


  //编写自定义函数，实现加密处理
  val hashMd5: UserDefinedFunction = udf((str: String) => {
    Md5.hashMD5(str + "shujia")
  })


  def main(args: Array[String]): Unit = {


    //创建spark环境
    spark = SparkSession
      .builder()
      .master("local")
      .appName(this.getClass.getSimpleName.replace("$", ""))
      //.enableHiveSupport() //开启hive的元数据支持
      .getOrCreate()



    //如果在sql厚葬要使用自定义函数需要注册
    spark.udf.register("hashMd5", hashMd5)


    /**
      * 调用子类的方法
      *
      */
    this.run(spark)


  }

  /**
    * 在子类中实现run方法，实现自定义的代码逻辑
    *
    * 在子类run方法的前面加上下面两行代码
    * import spark.implicits._
    * import org.apache.spark.sql.functions._
    *
    * @param spark spark的环境
    */
  def run(spark: SparkSession)


}
