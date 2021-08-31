package dev.vergil
package main

import org.apache.spark.sql.SparkSession

import scala.collection.breakOut
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

object AlgoritmKmeans {

  def mapUrls(e: String): (String, List[Double]) = {
    val split = e.split(";",2)
    val list: List[Double] = split(1)
      .split((";"))
      .map(e=>e.toDouble)(breakOut)

    (split(0), list)

  }

  def main(args: Array[String]): Unit = {

    val path = if (args.isEmpty) "mini" else "big"
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("kmenas")
      .getOrCreate()
    import spark.implicits._

    val vectors = spark.read.textFile( s"./in/$path/vector-norm").map(mapUrls)
    val ipsToDouble = vectors.map(e=>e._1).collect().zipWithIndex.map(a=>(a._1, a._2.toDouble)).toMap;

    val parsedData = vectors.map(e=>{

      val l = List[Double](ipsToDouble(e._1))
      val nova = l++e._2

      Vectors.dense(nova.toArray)
    })

//    val kmeans = new KMeans()
//      .setK(8)
//      .setFeaturesCol("features")
//      .setPredictionCol("prediction")



    vectors.collect().foreach(println)

  }


}
