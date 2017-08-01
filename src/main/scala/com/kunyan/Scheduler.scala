package com.kunyan

import java.text.SimpleDateFormat
import java.util.regex.Pattern

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * Created by lcm on 2016/12/30.
 *
 */
object Scheduler {


  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("shdx_offline_hk_stock ")
    //          .setMaster("local")

    val sc = new SparkContext(sparkConf)

    //金融平台的平台id
    val platformId = Array(
      "570",
      "568",
      "573",
      "562",
      "565",
      "565",
      "565",
      "558",
      "558",
      "564",
      "563",
      "563",
      "553",
      "553",
      "553",
      "569",
      "557",
      "561",
      "571",
      "559",
      "567",
      "567",
      "556",
      "549",
      "572",
      "551",
      "560",
      "582",
      "582",
      "570",
      "568",
      "573",
      "562",
      "565",
      "553",
      "561",
      "559",
      "556",
      "560",
      "582"
    )

    val urlMatches = Array(
      //搜索
      "quotes.money.163.com.*word=(.*)&t=",
      "sina.com.cn.*q=(\\d{5,6})",
      "xueqiu.*code=(.*)&size",
      "quote.cfi.cn.*keyword=(.*)&his",
      "app.stcn.com.*wd=(.*)&catid",
      "cy.stcn.com/S/(.*?)/",
      "dty.stcn.com.*searchCode=(\\d{5,6})",
      "quote.stockstar.com.*keyword=(\\d{5,6})",
      "so.stockstar.com.*q=(\\d{5,6})",
      "search.cnstock.com.*k=(\\d{5,6})",
      "search.cs.com.cn.*searchword=(.*?)&",
      "xinpi.cs.com.cn.*q=(\\d{5,6})",
      "search.10jqka.com.cn.*w=(\\d{5,6})",
      "activity.10jqka.com.cn.*/(\\d{5,6})",
      "stockpage.10jqka.com.cn/(\\d{5,6}?)/",
      "q.stock.sohu.com.*keyword=(.*?)&",
      "hq.p5w.net.*query=(.*?)&",
      "code.jrjimg.cn.*key=(.*?)&",
      "wallstreetcn.com.*q=(\\d{5,6})",
      "so.hexun.com.*key=(\\d{5,6})&",
      "search.ifeng.com.*q=(\\d{5,6})&",
      "app.finance.ifeng.com.*q=(\\d{5,6})&",
      "quote.eastmoney.com.*stockcode=(\\d{5,6})",
      "www.yicai.com.*searchKeyWords=(\\d{5,6})",
      "www.cailianpress.com.*keyword=(\\d{5,6})",
      "finance.21cn.com.*keywords=(\\d{5,6})",
      "search.caijing.com.cn.*key=(\\d{5,6})",
      "api.sugg.sogou.com.*key=(\\d{5,6})",
      "smartbox.gtimg.cn.*q=(\\d{5,6})&",
      //查看
      "quotes.money.163.com.*(\\d{5,6}).html",
      "stock.finance.sina.com.cn.*(\\d{5,6}).html",
      "xueqiu.com/S/(\\d{5,6})",
      "quote.cfi.cn.*searchcode=(\\d{5,6})",
      "dty.stcn.com.*searchCode=(\\d{5,6})",
      "stockpage.10jqka.com.cn/HK(\\d{4})/",
      "hk.jrj.com.cn/share/(\\d{5,6})/",
      "hkquote.stock.hexun.com.*(\\d{5,6}).shtml",
      "quote.eastmoney.com/hk/(\\d{5,6}).html",
      "stock.caijing.com.cn.*(\\d{5,6}).html",
      "gu.qq.com/hk(\\d{5,6})/"
    )

    val baseHost = sc.textFile(args(0))
      .collect().toList

    sc.textFile(args(1))
      .filter(x => {
        val arr = x.split("\\t")
        if (arr.size < 7) {
          false
        } else {
          val url = arr(3)
          val host = getHost(url)
          baseHost.contains(host)
        }
      })
      .map(x => getStockData(x,platformId,urlMatches))
      .filter(x =>{
      x != null && x._2 != "none"
      })
      .groupBy(x => (x._2, x._3, x._5))
      .flatMap(x => {
        val list = new ListBuffer[(String, String, String, String, String, String)]
        for (data <- x._2) {
          list.+=((data._1, data._4, data._5, data._6, x._2.size.toString, data._7))
        }
        list
      })
      .groupBy(x => x._1)
      .flatMap(x => {
        val list = new ListBuffer[String]
        var num = 1
        for (data <- x._2) {
          list.+=(data._1 + "=>" + num + "\t" + data._2 + "\t" + data._3 + "\t" + data._4 + "\t" + data._5 + "\t" + data._6)
          num = num + 1
        }
        list
      })
      .repartition(1)
      .saveAsTextFile(args(2))
//       .foreach(x => {
//        println(x)
//          })
  }




  //将url提取出host
  def getHost(url: String): String = {

    var host = url
    if (url.startsWith("http://"))
      host = url.replace("http://", "")
    if (url.startsWith("https://"))
      host = url.replace("https://", "")
    host = host.split("/")(0).split(":")(0)
    if (host.startsWith("www."))
      host = host.replace("www.", "")
    host
  }


  //获取股票热度数据
  def getStockData(data: String,platformId:Array[String],urlMatches:Array[String]): (String, String, String, String, String, String, String) = {

    val arr = data.split("\\t")
    val ad = arr(1)
    val url = arr(3)
    val time = arr(2)
    val min = getMin(time)
    val sec = getSec(time)

    try {
      for (index <- urlMatches.indices) {

        val pattern = Pattern.compile(urlMatches(index))
        val res = pattern.matcher(url)

        if (res.find()) {

          if (index == 34) {
            return (sec, min, ad, time, "0" + res.group(1), index.toString, platformId(index))
          } else {
            return (sec, min, ad, time, res.group(1), index.toString, platformId(index))
          }

        }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    null
  }

  //获取时间精确到分钟
  def getMin(time: String): String = {
    val df = new SimpleDateFormat("yyyyMMddHHmm")
    df.format(time.toLong)
  }

  //获取时间精确到秒钟
  def getSec(time: String): String = {
    val df = new SimpleDateFormat("yyyyMMddHHmmss")
    df.format(time.toLong)
  }
}
