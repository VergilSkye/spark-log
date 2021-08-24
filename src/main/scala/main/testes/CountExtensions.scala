package dev.vergil
package main.testes

import dev.vergil.util.{NginxLineParser, utilities}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CountExtensions {
  val baseRegex = ".+\\."
  val extensionsRegex = ".+\\/{2}.+\\/{1}.+(\\.\\w+)\\?*.*"

  def main(args: Array[String]): Unit = {
    val path = if (args.isEmpty) "big" else "big"
    utilities.setupLogging()
    val conf = new SparkConf().setAppName("URLCount")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input = sc.textFile(s"./in/${path}/access.log")
    val logs = input.map(line => NginxLineParser.parse(line))
    val urls: RDD[String] = logs.flatMap(e => if (e.isDefined) Some(e.get.request.URL) else None)

    val urlExtentions = urls.map(_.takeRight(30)).filter(_.contains(".")).map(e => {
      val ext = e.split(".+\\.", 2).filter(_.nonEmpty).map(e => e.split("&|\\?", 2)(0))
      (e, ext.mkString)
    })
    urlExtentions.saveAsTextFile(s"./in/${path}/url-extensions")


    val extensionsCount = urlExtentions.map(_._2).map(ext => (ext, 1)).reduceByKey((a, b) => a + b);
    val sortExtensionsCount = extensionsCount.sortBy(_._2, false);
    sortExtensionsCount.saveAsTextFile(s"./in/${path}/count-extensions")


    sc.stop()
  }
}
