package dev.vergil
package main

import dev.vergil.util.utilities
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

import java.io.PrintWriter
import scala.collection.breakOut

object AlgoritmKmeans {

  def mapUrls(e: String): (String, List[Double]) = {
    val split = e.split(";", 2)
    val list: List[Double] = split(1)
      .split((";"))
      .map(e => e.toDouble)(breakOut)

    (split(0), list)

  }

  def main(args: Array[String]): Unit = {
    val t1 = System.nanoTime
    utilities.setupLogging()
    val path = if (args.isEmpty) "mini" else "big"
    val conf = new SparkConf().setAppName("AlgoritmKmeans").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val vectors = sc.textFile(s"./in/$path/vector-norm").map(mapUrls)
    val ipsToDouble = vectors.map(e => e._1).collect().zipWithIndex.map(a => (a._1, a._2.toDouble)).toMap;

    val parsedData = vectors.map(e => {
      val l = List[Double](ipsToDouble(e._1))
      l ++ e._2
    })
    val parsedVector = parsedData.map(e => Vectors.dense(e.toArray))


    // Cluster the data into 10 classes using KMeans
    val numClusters = 35
    val numIterations = 15
    val clusters = KMeans.train(parsedVector, numClusters, numIterations)

    val predictCluster = parsedData.map { row => {
      val ip = ipsToDouble.find(_._2 == row(0)).get._1;
      val predict = clusters.predict(Vectors.dense(row.toArray))
      (ip, predict)
    }

    }
    val a = predictCluster.map(e=> (e._2,e._1)).groupByKey().map(e=> {
      (e._2.toList).mkString(";")
    })

    a.coalesce(1,true).saveAsTextFile(s"./in/$path/kmeans-result")
    sc.stop()

    val duration = (System.nanoTime - t1) / 1e9d
    println(s"essa foi a duração: $duration")

    new PrintWriter(s"./in/${path}/duration-AlgoritmKmeans.txt") {
      write(s"duration: $duration");
      close()
    }

  }


}
