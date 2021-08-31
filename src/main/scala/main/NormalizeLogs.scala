package dev.vergil
package main

import main.VectorLogs.{generateArrayValues, generateVector, listaDeObjectos, mapUrls, outputFormat}
import util.{NginxLineParser, utilities}

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.breakOut


object NormalizeLogs {
  def mapUrls(e: String): List[Double] = {
    val b = e.split("\\|")(0).split(";",2)
    val l1:List[Double] = b(1).split(";").map(e=>e.toDouble)(breakOut)
    val l2: List[Double] = e.split("\\|")(1).split(";").map(e=>e.toDouble)(breakOut)

    l1++l2
  }

  def main(args: Array[String]) {
    val path = if (args.isEmpty) "mini" else "big"
    utilities.setupLogging()
    val conf = new SparkConf().setAppName("NormalizeLog").setMaster("local[*]")
    val sc = new SparkContext(conf)


    val vectorParams = sc.textFile(s"./in/$path/vector-params/part-*").map(e => mapUrls(e))
    val l = vectorParams.collect().map(e=>e.toArray).transpose
    l.foreach(a=>a.mkString(","))




//    val input = sc.textFile(s"./in/$path/filter-urls/part-*")
//    val logs = input.map(line => NginxLineParser.parse(line))
//    val filterData = logs.flatMap(e => if (e.isDefined) Some(generateArrayValues(e.get, bKnowUlrs)) else None)
//
//    val info = filterData.groupBy(e => e.ip)
//      .map(e => listaDeObjectos(e._2.toList))
//      .map(e => generateVector(e,knowUrls.size ))
//      .map(e=>outputFormat(e))
//
//    info.coalesce(1,true).saveAsTextFile(s"./in/$path/vector-params")

    //
    //    val ipUrls: RDD[(String, String)] = logs.flatMap(e => if (e.isDefined) Some((e.get.remoteAddr, e.get.request.URL)) else None)
    //    val uniqueIps = urls.distinct
    //
    //
    //

    sc.stop()
  }
}
