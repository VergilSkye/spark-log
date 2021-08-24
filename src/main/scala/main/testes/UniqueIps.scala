package dev.vergil
package main.testes

import util.{NginxLineParser, utilities}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.PrintWriter

object UniqueIps {
  def main(args: Array[String]) {
    val path = if (args.isEmpty) "mini" else "big"
    utilities.setupLogging()
    val conf = new SparkConf().setAppName("UniqueIps").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input = sc.textFile(s"./in/${path}/access.log")
    val logs = input.map(line => NginxLineParser.parse(line))
    val urls: RDD[String] = logs.flatMap(e => if (e.isDefined) Some(e.get.remoteAddr) else None)
    val ipUrls: RDD[(String, String)] = logs.flatMap(e => if (e.isDefined) Some((e.get.remoteAddr, e.get.request.URL)) else None)
    val uniqueIps = urls.distinct


    uniqueIps.saveAsTextFile(s"./in/${path}/unique-ips")
    new PrintWriter(s"./in/${path}/total-ips.txt") {
      write(uniqueIps.count.toString);
      close()
    }

    sc.stop()
  }
}
