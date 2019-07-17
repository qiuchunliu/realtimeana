package utils

import java.text.SimpleDateFormat


/**
  * 将字符串时间解析为时间戳
  * 也可以直接返回时间差
  */
object ParseTime {

  // 20170412030031554
  def parse(str: String): Long ={
    val datestr: String = str.substring(0, 17)  // 取出表示时间的字符串
    val timestamp: Long= new SimpleDateFormat("yyyyMMddHHmmssSSS").parse(datestr).getTime
    timestamp   // 返回时间戳
  }

}
