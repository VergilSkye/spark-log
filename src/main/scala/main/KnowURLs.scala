package dev.vergil
package main

import util.{NginxLineParser, utilities}

import org.apache.spark.{SparkConf, SparkContext}

import java.util.regex.Pattern
import scala.sys.process._

object KnowURLs {
  def main(args: Array[String]): Unit = {
    val t1 = System.nanoTime
    utilities.setupLogging()
    val path = if (args.isEmpty) "mini" else "big"
    val conf = new SparkConf().setAppName("KnowUrls").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input = sc.textFile(s"./in/$path/filter-urls/part-*")
    val knowUrls = input.map(convertRawUrlToPruner).distinct.zipWithIndex().map(e => e._1 + "|" + e._2)


    knowUrls.coalesce(1,true).saveAsTextFile(s"./in/$path/know-urls")
    sc.stop()

    val duration = (System.nanoTime - t1) / 1e9d
    println(s"essa foi a duração: $duration")


  }
  def convertRawUrlToPruner(rawUrl: String): String = {
    urlPruner(extractUrlOnly(rawUrl))
  }

 def urlPruner(url: String): String = {
   val filterRegex = "((?=.*?[A-Za-z])(?=.*?[0-9])|(?=.*?(\\.[a-z]+))|(?=.*?[?\\-_=+*&%$#@!])).*"
   val slashMatcher = Pattern.compile("(\\/[^\\/]+)").matcher(url)
   var url_f = ""
   while(slashMatcher.find()) {
     val s = slashMatcher.group(1)
     if (!s.matches(filterRegex)) {
       if(s.matches("(\\/\\d+)")) url_f += "/id_num";
       else {
         val temp = if (s.isBlank) "" else s;
         url_f +=temp
       }
     }
   }
   url_f.replaceAll("/null", "");

 }

  private def extractUrlOnly(rawUrl: String) = {
    val NginxLineParser.regex(ip, client, datetime, url, status, bytes, referer, agent) = rawUrl
    val split = url.split(" ").toList match {
      case List(a, b, c) => Some((a, b, c))
      case other => None
    }
    val line = split.get._2;
    line
  }

}
