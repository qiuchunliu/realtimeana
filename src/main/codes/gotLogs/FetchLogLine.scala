package gotLogs

import java.sql.Connection

import com.alibaba.fastjson.JSONObject
import constant.Constant
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis
import utils.{ConnectPoolUtils, OffsetInRedis}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}


object FetchLogLine {

  def fetchSingleLogLine(){
    // 获取 sparkstreaming

    val conf: SparkConf = new SparkConf().setAppName("real").setMaster("spark://192.168.163.21:7077")
    conf      // 设置每秒钟每个分区拉取kafka的速率
      .set("spark.streaming.kafka.maxRatePerPartition","100")
      // 设置序列化机制
      .set("spark.serlizer","org.apache.spark.serializer.KryoSerializer")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    /*
     * 配置基本参数
     */
    val kafkas: Map[String, Object] = Constant.fetchKafkas()
      // 创建topic集合
      val topics: Set[String] = Set(Constant.TOPIC)
    val partitionToLong: Map[TopicPartition, Long] = OffsetInRedis.apply(Constant.GROUPID)

    val stream :InputDStream[ConsumerRecord[String,String]] =  // InputDStream 继承了 DStream
      if(partitionToLong.isEmpty){
        KafkaUtils.createDirectStream(ssc,
          // 本地策略
          // 将数据均匀的分配到各个Executor上面
          LocationStrategies.PreferConsistent,
          // 消费者策略
          // 可以动态增加分区
          ConsumerStrategies.Subscribe[String,String](topics,kafkas)
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

    stream.foreachRDD({
      rdd=>
        val offestRange: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 不要将这个对象传出去，数据库连接不能传
        val jedis: Jedis = ConnectPoolUtils.getJedis
        // 从连接池获取连接
        val connections: Connection = ConnectPoolUtils.getConnections
        // 业务处理
        val jsRdd: RDD[JSONObject] = ParseLogs.splitLog(rdd)
        jsRdd.cache()

        // 每天的订单量、成交额、成功量、总耗时
        val values: RDD[(String, List[Double])] = ParseLogs.topUp(jsRdd)
        // 每分钟的订单量
        val values_permin: RDD[(String, Int)] = ParseLogs.orders_permin(jsRdd)
        // 将每天的订单量、成交额、成功量、总耗时数据存入redis
        ParseLogs.saveStageOne(values)
        // 每分钟的数据存储
        ParseLogs.save_permin(values_permin)
        // 各省每小时的失败订单量
        val values_failureorders_per_cityday: RDD[((String, String), Int)] =
          ParseLogs.failure_orders_per_city_per_day(jsRdd)
        // 将各省每小时的失败订单量统计存储
        ParseLogs.save_failure_orders_per_city_per_day(values_failureorders_per_cityday)
        /**
          * 获取订单的日期(精确到分钟)、省份、充值是否成功、成功充值的金额
          * 存入mysql
          */
        val values_res4: RDD[((String, String), List[Double])] = ParseLogs.res4(jsRdd)
        ParseLogs.res(values_res4)

        // 将偏移量进行更新
        for (or <- offestRange){
          jedis.hset(Constant.GROUPID, or.topic + "-" + or.partition, or.untilOffset.toString)
        }

        // 及时关闭
        jedis.close()
        connections.close()
    })
    // 启动
    ssc.start()
    ssc.awaitTermination()
}}
