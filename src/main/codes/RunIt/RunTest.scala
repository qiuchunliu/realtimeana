package RunIt

import com.alibaba.fastjson.{JSON, JSONObject}
import constant.ConstantCity
import gotLogs.FetchLogLine
import utils.ParseTime

object RunTest {

  def main(args: Array[String]): Unit = {

//    FetchLogLine.fetchSingleLogLine()

    val t: JSONObject = JSON.parseObject("{\"bussinessRst\":\"0000\",\"channelCode\":\"0705\",\"charg" +
      "efee\":\"10000\",\"clientIp\":\"60.168.242.11\",\"endReqTime\":\"20170412055" +
      "958007\",\"idType\":\"01\",\"interFacRst\":\"0000\",\"logOutTime\":\"20170412055958" +
      "007\",\"orderId\":\"384674389168703891\",\"prodCnt\":\"1\",\"provinceCode\":\"250\",\"requ" +
      "estId\":\"20170412055949778239860387164527\",\"retMsg\":\"成功\",\"serve" +
      "rIp\":\"172.16.59.241\",\"serverPort\":\"8088\",\"serviceName\":\"sendRechar" +
      "geReq\",\"shouldfee\":\"9950\",\"startReqTime\":\"20170412055957895\",\"sysId\":\"15\"}")

    println(ConstantCity.CITYMAP.getOrElse(t.getString("provinceCode"), "none"))

  }

}
