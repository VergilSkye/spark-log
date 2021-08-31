package dev.vergil
package main

import main.VectorLogs.{generateArrayValues, generateVector, listaDeObjectos, mapUrls, outputFormat}
import util.{NginxLineParser, utilities}

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.breakOut


object NormalizeLogs {
  def mapUrls(e: String): (String, List[Double]) = {
    val split = e.split(";",2)
    val list: List[Double] = split(1)
      .split((";"))
      .map(e=>e.toDouble)(breakOut)

    (split(0), list)

  }

  def normalize(e: (String, List[Double]), g: Array[(Double, Int, Double)]): (String, List[Double]) = {
    val norms = e._2.zipWithIndex.map(a => {
      val index = a._2

      val sum=g(index)._1
      val total=g(index)._2
      val variance=g(index)._3

      if(total==0 || variance==0.0) {
         0.0
      } else {
        Math.abs(a._1-(sum/total))/variance
      }
    })

    (e._1, norms)
  }

  def main(args: Array[String]) {
    val path = if (args.isEmpty) "mini" else "big"
    utilities.setupLogging()
    val conf = new SparkConf().setAppName("NormalizeLog").setMaster("local[*]")
    val sc = new SparkContext(conf)


    val vectorParams = sc.textFile(s"./in/$path/vector-params/part-*").map(e => mapUrls(e))
    val l = vectorParams.collect().map(e=>e._2.toArray).transpose
    val vector = sc.parallelize(l)
    val norm = vector.map(e=> {
      val a = ( e.sum, e.length)
      val mean = e.sum/e.length
      var top = 0.0;
      for(ele <-e) {
        top += Math.pow(ele-mean, 2)
      }

      val variance = top/e.length
      (a._1, a._2, Math.sqrt(variance));
    })
    norm.map(e=> s"${e._1};${e._2};${e._3}").coalesce(1,true).saveAsTextFile(s"./in/$path/norm")

    val g = norm.collect()
    val normVector = vectorParams.map(e=>normalize(e,g))
    normVector.map(e=> s"${e._1};${e._2.mkString(";")}").coalesce(1,true).saveAsTextFile(s"./in/$path/vector-norm")


    sc.stop()
  }
}
