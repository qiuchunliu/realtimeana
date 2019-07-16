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
    getsk().sparkContext.getConf
  }
  def getStreamingContext: StreamingContext ={
    new StreamingContext(getconf(), Seconds(5))
  }

}
