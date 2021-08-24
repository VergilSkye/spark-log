package dev.vergil
package main

import util.{NginxLineParser, utilities }

import org.apache.spark.{SparkConf, SparkContext}

import java.io.PrintWriter

object FilterRawLog {
  def main(args: Array[String]): Unit = {
    val t1 = System.nanoTime
    utilities.setupLogging()
    val path = if (args.isEmpty) "big" else "big"
    val conf = new SparkConf().setAppName("FilterRawLog").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input = sc.textFile(s"./in/$path/access.log")
    val logs = input.filter(urlFromHumans)
      .filter(e => wantedWords(e, List("/product/")))
//      .filter(excludeExtensions)
      .filter(e => unwantedWords(e, "image,filter,search,rss".split(",").toList))


    logs.saveAsTextFile(s"./in/$path/filter-urls")
    new PrintWriter(s"./in/$path/filter-total-urls.txt") {
      write(logs.count.toString)
      close()
    }
    sc.stop()

    val duration = (System.nanoTime - t1) / 1e9d
    println(s"essa foi a duração: $duration")
  }

  def urlFromHumans(url: String): Boolean = {
    !url.contains("bot")
  }

  def excludeExtensions(url: String): Boolean = {
    val extensions = List(".png", ".jpg", ".woff", ".js", ".ico", ".css", ".gif", ".ttf", ".json", ".eot", ".txt", ".php", "/settings/logo")
    !(extensions exists url.contains)
  }

  def wantedWords(url: String, wantedWords: List[String]): Boolean = {
    // https://alvinalexander.com/scala/how-to-extract-parts-strings-match-regular-expression-regex-scala/
    val NginxLineParser.regex(ip, client, datetime, req, status, bytes, referer, agent) = url
    wantedWords exists req.contains
  }

  def unwantedWords(url: String, unwantedWords: List[String]): Boolean = {
    !(unwantedWords exists url.contains)
  }


}
