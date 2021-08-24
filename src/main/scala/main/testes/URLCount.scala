package dev.vergil
package main.testes

import util.{NginxLineParser, utilities}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object URLCount {
  def main(args: Array[String]) {
    val path = if (args.isEmpty) "mini" else "big"
    utilities.setupLogging()
    val conf = new SparkConf().setAppName("URLCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input = sc.textFile(s"./in/${path}/access.log")
    val logs = input.map(line => NginxLineParser.parse(line))
    val urls: RDD[String] = logs.flatMap(e => if (e.isDefined) Some(e.get.request.URL) else None)
    val urlCounts = urls.map(url => (url, 1)).reduceByKey((a, b) => a + b);

    val sortUrlCounts = urlCounts.sortBy(_._2, false);
    sortUrlCounts.saveAsTextFile(s"./in/${path}/count-urls")

    sc.stop()
  }
}
