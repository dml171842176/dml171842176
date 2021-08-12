package com.shujia.util

import java.security.MessageDigest

object Md5 {
  def hashMD5(content: String): String = {
    val md5: MessageDigest = MessageDigest.getInstance("MD5")
    val encoded: Array[Byte] = md5.digest(content.getBytes)
    encoded.map("%02x".format(_)).mkString.toUpperCase
  }

  def main(args: Array[String]) {
    println(hashMD5("17600195923javasdasd"))
    println(hashMD5("abcde"))
  }

}