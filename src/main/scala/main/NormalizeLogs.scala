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
    val vector = sc.parallelize(l)
    val a = vector.map(e=> {
      val a = ( e.sum, e.length)
      val mean = e.sum/e.length
      var top = 0.0;
      for(ele <-e) {
        top += Math.pow(ele-mean, 2)
      }

      val variance = top/e.length
      (a._1, a._2, Math.sqrt(variance));
    })
    a.collect().foreach(println)



    sc.stop()
  }
}
