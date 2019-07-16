package utils

import java.util

import org.apache.kafka.common.TopicPartition
import redis.clients.jedis.Jedis

object OffsetInRedis {

  def apply(groupId: String):Map[TopicPartition,Long] = {
    // 创建最后的返回值类型
    var formdbOffset: Map[TopicPartition, Long] = Map[TopicPartition,Long]()
    // 创建Jedis连接
    val jedis: Jedis = ConnectPoolUtils.getJedis
    // 查询redis中所有的Topic、Partition
    val topicPartitionOffset: util.Map[String, String] = jedis.hgetAll(groupId)
    // 需要执行隐式转换操作
    import scala.collection.JavaConversions._
    // 将map转换list进行循环处理         hz1803a-1 888
    val topicPartitionOffsetList: List[(String, String)] = topicPartitionOffset.toList
    // 循环处理数据
    for (topicPL <- topicPartitionOffsetList){
      val str: Array[String] = topicPL._1.split("[-]")
      formdbOffset += (new TopicPartition(str(0),str(1).toInt)->topicPL._2.toLong)
    }
    formdbOffset
  }

}
