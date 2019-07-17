package utils

import java.text.SimpleDateFormat


/**
  * 将字符串时间解析为时间戳
  * 也可以直接返回时间差
  */
object ParseTime {

  // 20170412030031554
  def parse(str: String): Long = {
    var timestamp = 0L
    if (str != null) {
      val datestr: String = str.substring(0, 17) // 取出表示时间的字符串
      timestamp = new SimpleDateFormat("yyyyMMddHHmmssSSS").parse(datestr).getTime
    }
    timestamp // 返回时间戳
  }

}
