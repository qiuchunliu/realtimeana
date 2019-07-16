package gotLogs

import java.lang

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis
import utils.{ConnectPoolUtils, OffsetInRedis}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}


object FetchLogLine {

  def fetchSingleLogLine(): Unit ={
    // 获取 sparkstreaming
    val conf: SparkConf = new SparkConf().setAppName("real").setMaster("local[2]")
    conf      // 设置没秒钟每个分区拉取kafka的速率
      .set("spark.streaming.kafka.maxRatePerPartition","100")
      // 设置序列化机制
      .set("spark.serlizer","org.apache.spark.serializer.KryoSerializer")
//    val ssc: StreamingContext = Constant.getStreamingContext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    /*
     * 配置基本参数
     */

    // 消费者组
    val groupID = "realtimeana_consumer"
    // 生产者主题
    val topic = "realtimeana"
    // 指定brocker地址
    val brockerList = "192.168.163.21:9092"
    // 配置kafka参数
    val kafkas: Map[String, Object] = Map[String,Object](
      "bootstrap.servers"->brockerList,
      // kafka的Key和values解码方式
      "key.deserializer"-> classOf[StringDeserializer],
      "value.deserializer"-> classOf[StringDeserializer],
      "group.id"->groupID,
      // 从头消费
      "auto.offset.reset"-> "earliest",
      // 不需要程序自动提交Offset
      "enable.auto.commit"-> (false:lang.Boolean)
    )
      // 创建topic集合
//      val topics: Set[String] = Set(topic)
      val topics: Map[String, Int] = Map(topic -> 1)
    val partitionToLong: Map[TopicPartition, Long] = OffsetInRedis.apply(groupID)

    val stream :InputDStream[ConsumerRecord[String,String]] =  // InputDStream 继承了 DStream
      if(partitionToLong.isEmpty){
        KafkaUtils.createDirectStream(ssc,
          // 本地策略
          // 将数据均匀的分配到各个Executor上面
          LocationStrategies.PreferConsistent,
          // 消费者策略
          // 可以动态增加分区
          ConsumerStrategies.Subscribe[String,String](topics.keys,kafkas)
        )
      }else{
        // 不是第一次消费
        KafkaUtils.createDirectStream(
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Assign[String,String](partitionToLong.keys,kafkas,partitionToLong)
        )
      }

    /*
     * 业务处理部分
     */

    stream.cache()
    stream.foreachRDD({
      rdd=>
        val offestRange: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val jedis: Jedis = ConnectPoolUtils.getJedis
        // 业务处理
//        rdd.map(_.value()).foreach(println)
        val jsRdd: RDD[JSONObject] = ParseLogs.splitLog(rdd)
        val values: RDD[(String, List[Double])] = ParseLogs.topUp(jsRdd)
        val values_permin: RDD[(String, Int)] = ParseLogs.orders_permin(jsRdd)
        // 将数据存入redis
        ParseLogs.saveStageOne(values, jedis)
        // 每分钟的数据存储
        ParseLogs.save_permin(values_permin, jedis)
        val values_failureorders_per_cityday: RDD[((String, String), Int)] = ParseLogs.failure_orders_per_city_per_day(jsRdd)
        // 将各省每小时的失败订单量统计存储
        ParseLogs.save_failure_orders_per_city_per_day(values_failureorders_per_cityday)


        // 将偏移量进行更新
        for (or <- offestRange){
          jedis.hset(groupID, or.topic + "-" + or.partition, or.untilOffset.toString)
        }
        jedis.close()
    })
    // 启动
    ssc.start()
    ssc.awaitTermination()

  }

}
