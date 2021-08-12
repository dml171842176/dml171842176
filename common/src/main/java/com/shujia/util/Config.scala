package com.shujia.util

import java.io.InputStream
import java.util.Properties

object Config {

  //1、读取default.properties配置文件

  val properties = new Properties()

  //2、从resources目录下获取一个输入流
  val inputStream: InputStream = this.getClass.getClassLoader.getResourceAsStream("default.properties")

  //3、加载配置文件
  properties.load(inputStream)


  def get(key: String): String = {
    properties.getProperty(key)
  }


}
