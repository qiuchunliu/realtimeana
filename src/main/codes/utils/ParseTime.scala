package utils

import java.text.SimpleDateFormat


/**
  * 将字符串时间解析为时间戳
  */
object ParseTime {

  // 20170412030031554
  // 2017-04-12 03:00:13.393 282687799171031
  def parse(str: String): Long ={
    val datestr: String = str.substring(0, 4) + "-" +
      str.substring(4, 6) + "-" +
      str.substring(6, 8) + " " +
      str.substring(8, 10) + ":" +
      str.substring(10, 12) + ":" +
      str.substring(12, 14) + "." +
      str.substring(14)
    println(datestr)
    println(str.substring(0, 12))  // 以分钟为单位
    val timestamp: Long= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(datestr).getTime
    timestamp
  }

}
