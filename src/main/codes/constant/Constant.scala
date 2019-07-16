package constant

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

}
