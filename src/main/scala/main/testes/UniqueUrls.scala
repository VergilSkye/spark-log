package dev.vergil
package main.testes

import util.{NginxLineParser, utilities}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.PrintWriter

object UniqueUrls {
  def main(args: Array[String]) {
    val t1 = System.nanoTime
    utilities.setupLogging()
    val path = if (args.isEmpty) "mini" else "big"
    val conf = new SparkConf().setAppName("UniqueUrls").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input = sc.textFile(s"./in/${path}/access.log")
    val logs = input.map(line => NginxLineParser.parse(line))
    val urls: RDD[String] = logs.flatMap(e => if (e.isDefined) Some(e.get.request.URL) else None)
    val uniqueUrls = urls.distinct
    uniqueUrls.saveAsTextFile(s"./in/${path}/unique-urls")
    new PrintWriter(s"./in/${path}/total-urls.txt") {
      write(uniqueUrls.count.toString);
      close()
    }

    sc.stop()
    val duration = (System.nanoTime - t1) / 1e9d
    println(s"essa foi a duração: $duration")
  }
}
