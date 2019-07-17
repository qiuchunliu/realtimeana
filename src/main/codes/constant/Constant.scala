package constant

import java.lang

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Constant {

  def getsk(): SparkSession ={
    val sk: SparkSession = SparkSession.builder().appName("realtimeana").master("local[4]").getOrCreate()
    sk
  }

  def getconf(): SparkConf ={
    getsk().sparkContext.getConf      // 设置没秒钟每个分区拉取kafka的速率
      .set("spark.streaming.kafka.maxRatePerPartition","100")
      // 设置序列化机制
      .set("spark.serlizer","org.apache.spark.serializer.KryoSerializer")
  }
  def getStreamingContext: StreamingContext ={
    new StreamingContext(getconf(), Seconds(5))
  }

  def fetchKafkas(): Map[String, Object] ={
    val kafkas: Map[String, Object] = Map[String,Object](
      "bootstrap.servers"->Constant.BROCKERLIST,
      // kafka的Key和values解码方式
      "key.deserializer"-> classOf[StringDeserializer],
      "value.deserializer"-> classOf[StringDeserializer],
      "group.id"->Constant.GROUPID,
      // 从头消费
      "auto.offset.reset"-> "earliest",
      // 不需要程序自动提交Offset
      "enable.auto.commit"-> (false:lang.Boolean)
    )
    kafkas
  }


  // 消费者组
  val GROUPID = "realtimeana_consumer"
  // 设置主题
  val TOPIC = "realtimeana"
  // 块
  val BROCKERLIST = "192.168.163.21:9092"

}
