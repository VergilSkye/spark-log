package dev.vergil
package main

import dev.vergil.util.{NginxLineParser, utilities}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UrlByIP {
  def main(args: Array[String]) {
    utilities.setupLogging()
    val path = if (args.isEmpty) "mini" else "big"
    val conf = new SparkConf().setAppName("URLbyIp").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input = sc.textFile(s"./in/${path}/access.log")
    val logs = input.map(line => NginxLineParser.parse(line))
    val ipUrls: RDD[(String, String)] = logs.flatMap(e => if (e.isDefined) Some((e.get.remoteAddr, e.get.request.URL)) else None)
    val ipListUrl = ipUrls.groupByKey().map {
      case (a, b) => (a, b.toList)
    }

    ipListUrl.saveAsTextFile(s"./in/${path}/ip-urls")

    sc.stop()
  }
}
