package scala

import java.sql.{Connection, Statement}

import com.alibaba.fastjson.{JSON, JSONObject}
import constant.{Constant, ConstantCity}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis
import utils.{ConnectPoolUtils, ParseTime}

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

  /*
   * 每天的订单量、成交额、成功订单量。。。
   */
  def topUp(rdd: RDD[JSONObject]): RDD[(String, List[Double])] ={
    val singleOrder: RDD[(String, List[Double])] = rdd.map(e => {
      // 每个e都是一条订单
      // 获取日期  如：20170412
      val requestID: String = e.getString("requestId").substring(0, 8)
      //  业务是否成功，即充值是否成功
      val isCharged: Int = if(e.getString("bussinessRst").equals("0000")) 1 else 0
      // 获取充值金额
      // 注意：  只有订单成功了才有订单的成交金额
      val chargeFee: Double = if(isCharged == 1) e.getString("chargefee").toDouble else 0
      // 订单发起时间 调方法得到时间戳
      val startTime: Long = ParseTime.parse(e.getString("requestId"))
      // 收到订单完成的时间 调方法得到时间戳
      val endTime: Long = ParseTime.parse(e.getString("receiveNotifyTime"))
      val costTime: Long = endTime - startTime
      (requestID, List[Double](1, chargeFee, isCharged, costTime))
    })
    val restemp: RDD[(String, List[Double])] = singleOrder.reduceByKey((l1, l2) => {
      l1.zip(l2).map(t => {
        t._1+ t._2
      })
    })
    restemp
  }

  /*
   * 每分钟的订单量
   */
  def orders_permin(rdd: RDD[JSONObject]): RDD[(String, Int)] ={
    val singleOrder: RDD[(String, Int)] = rdd.map(e => {
      // 每个e都是一条订单
      // 获取分钟  如：201704120302
      val per_min: String = e.getString("requestId").substring(0, 12)
      (per_min, 1)
    })
    singleOrder.reduceByKey(_+_)
  }

  /**
    * 将每日的数据存入redis
    * @param v 待存数据
    * @param jedis 存入redis
    */
  def saveStageOne(v: RDD[(String, List[Double])], jedis: Jedis): Unit ={
    v.foreachPartition(p => {
      p.map(e => {
        // e -> (日期， list（总订单量，成功缴费金额，订单是否成功，订单耗时）)
        // 每日订单量
        jedis.hincrBy(e._1, "orders_eachday", e._2.head.toInt)  // .head取第一个值  .tail取非第一个值
        // 每日成功订单量
        jedis.hincrBy(e._1, "suc_orders_eachday", e._2(2).toInt)
        // 每日成交金额
        jedis.hincrByFloat(e._1, "amount_eachday", e._2(1).toLong)
        // 每日订单耗时
        jedis.hincrByFloat(e._1, "cost_time_eachday", e._2(3).toLong)
      })
    })
  }

  /**
    * 将每分钟的数据存入redis
    * @param v  以分钟数为key， 1 为value
    * @param jedis 存入redis
    */
  def save_permin(v: RDD[(String, Int)], jedis: Jedis): Unit ={
    v.foreachPartition(p => {
      p.map(e => {
        jedis.hincrBy(e._1.substring(0, 8), e._1.substring(8, 12), e._2)
      })
    })
  }

  /**
    * 各省每小时失败的订单量
    * @param rdd log的json字符串
    */
  def failure_orders_per_city_per_day(rdd: RDD[JSONObject]): RDD[((String, String), Int)] ={
    rdd.map(e => {
      // 小时数
      val per_hour: String = e.getString("requestId").substring(0, 10)
      // 是否充值成功
      // 如果成功 返回 0， 如果失败 返回 1   统计失败数
      val issuc: Int = if(e.getString("bussinessRst").equals("0000")) 0 else 1
      // 所属省份
      val city: String = ConstantCity.CITYMAP.getOrElse(e.getString("provinceCode"), "none")
      ((per_hour, city), issuc)
    }).reduceByKey(_+_)  // 以时间和省份为key，订单是否成功为 value
  }

  /**
    * 各省每小时失败订单量
    * 将数据存入mysql
    * @param v 各省每小时的失败订单数据
    *          v : ((日期，城市)， 失败数量)
    */
  def save_failure_orders_per_city_per_day(v: RDD[((String, String), Int)], connections: Connection): Unit ={

    // 创建语句
    val statement: Statement = connections.createStatement()
    var sql = "" // 记得改一下数据库

    v.map(e => {
      sql =
        "insert into " +
          "failure_perdayprov(order_date,province,counts) " +
          "values('" + e._1._1 + "','" + e._1._2 + "'," + e._2 + ");"
      statement.execute(sql)
    })
  }

  // 充值订单省份top10
  // 以省份为维度，每天的订单数、成功数、(成功率)
  // 实时统计每小时的充值笔数和充值金额
  /**
    * 获取订单的日期(精确到小时)、省份、充值是否成功、成功充值的金额
    * @param rdd  jsonObject
    * @param connections 存入mysql
    * @return
    */
  def res4(rdd: RDD[JSONObject], connections: Connection): RDD[Unit] = {
    val statement: Statement = connections.createStatement()
    var sql = ""
    rdd.map(e => {
      // 获取日期,精确到小时  如：2017041203
      val requestID: String = e.getString("requestId").substring(0, 10)
      //  业务是否成功，即充值是否成功
      val isCharged: Int = if (e.getString("bussinessRst").equals("0000")) 1 else 0
      // 获取充值金额
      // 注意：  只有订单成功了才有订单的成交金额
      val chargeFee: Double = if (isCharged == 1) e.getString("chargefee").toDouble else 0
      // 获取省份
      val province: String = ConstantCity.CITYMAP.getOrElse(e.getString("provinceCode"), "none")
      ((requestID, province),List[Double](isCharged, chargeFee))
    }).reduceByKey((l1, l2) => {
      l1.zip(l2).map(e => e._1 + e._2)  // 对成功订单量进行累加，对成功充值金额进行累加
    }).map(t => {
      sql = "insert into res4(order_date,province,isCharged,chargeFee) " +
        "values('"+t._1._1+"','"+t._1._2+"',"+t._2.head+","+t._2(1)+");"
      statement.executeUpdate(sql)
    })
  }









}
