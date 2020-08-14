package com.foxconn.test

import com.foxconn.util.configUtil

object argprint {
  def main(args: Array[String]): Unit = {
    println(configUtil.getValueFromConfig("kafka.broker.list"))
  }
}
