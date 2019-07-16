package scala

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD

/**
  * 解析json日志，获取所需字段数据
  */

object ParseLogs {

  def splitLog(rdd: RDD[ConsumerRecord[String, String]]): RDD[JSONObject] ={
    val rddjson: RDD[JSONObject] = rdd.map(e => {
      // 每一条为一个字符串
      val str: String = e.value()
      // fastjson接卸字符串，生成一个对象
      val js: JSONObject = JSON.parseObject(str)
      js
    })
    rddjson
  }
}
