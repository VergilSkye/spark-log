package dev.vergil
package main

import util._

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.Locale
import scala.collection.mutable.ListBuffer

object VectorLogs {

  val sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
  val datetime_format = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss")

  def generateArrayValues(e: NginxLogRecord, lista: Broadcast[Map[String, Int]]): FilterObject = {
    val ip = e.remoteAddr

    val epoch = LocalDateTime.from(datetime_format.parse(e.timeLocal.toLowerCase)).atZone(ZoneId.systemDefault).toEpochSecond

    val httpVerb = e.request.verb
    val urlPrune = KnowURLs.urlPruner(e.request.URL)
    val exists = lista.value.get(urlPrune)
    val indexPrune: Int = exists.getOrElse(0)
    val status = e.status
    val bytes = e.receivedBytes

    FilterObject(ip, epoch, httpVerb, indexPrune, status.toInt, bytes.toLong)

  }

  def mapUrls(e: String): (String, Int) = {
    val s = e.split("\\|")
    (s(0), s(1).toInt)
  }

  def listaDeObjectos(lista: List[FilterObject]): ListFilterObject = {
    val ip = lista(0).ip
    var epochs = new ListBuffer[Long]()
    var httpVerbs = new ListBuffer[String]()
    var prunes = new ListBuffer[Int]()
    var status = new ListBuffer[Int]()
    var bytes = new ListBuffer[Long]
    lista.foreach(w => {
      epochs += w.epoch
      httpVerbs += w.httpVerb
      prunes += w.indexPrune
      status += w.status;
      bytes += w.bytes
    })
    ListFilterObject(ip, epochs.toList, httpVerbs.toList, prunes.toList, status.toList, bytes.toList);


  }

  def tbmnaosei(sorted: List[String]) = {
    sorted.groupBy(identity).mapValues(_.size)

  }

  def tosabendo(sorted: List[Int]) = {
    sorted.map(e => {
      val n = e;
      if (n < 200) 100
      else if (n < 300) 200
      else if (n < 400) 300
      else if (n < 500) 400
      else 500
    }).groupBy(identity).mapValues(_.size)
  }

  def agorasim(sorted: List[Long]) = {
    sorted.sum

  }

  def seraquevai(indexPrune: List[Int], sizeUrlColumns: Int) = {
    val map = indexPrune.groupBy(identity).mapValues(_.size)
    val seq = Array.fill(sizeUrlColumns)(0)
    for (elem <- map) {
      seq.update(elem._1, elem._2)
    }
    seq
  }

  def generateVector(e: ListFilterObject, sizeUrlColumns:Int=0 ): Columns = {
    val dadosEpoch = aindaNaoSei(e.listEpoch.sorted, e.ip)
    val dadosHttpVerb = tbmnaosei(e.httpVerb.sorted)
    val dadosStatus = tosabendo(e.status)
    val dadosBytes = agorasim(e.bytes)
    val dadosColunasUrl = seraquevai(e.indexPrune, sizeUrlColumns)

    val a =
      Columns(
        e.ip,
        e.listEpoch.size.toString,
        dadosEpoch._3.toString,
        dadosEpoch._2.toString,
        dadosEpoch._1.toString,
        dadosStatus.getOrElse(100, 0).toString,
        dadosStatus.getOrElse(200, 0).toString,
        dadosStatus.getOrElse(300, 0).toString,
        dadosStatus.getOrElse(400, 0).toString,
        dadosStatus.getOrElse(500, 0).toString,
        dadosBytes.toString,
        dadosHttpVerb.getOrElse("GET", 0).toString,
        dadosHttpVerb.getOrElse("PUT", 0).toString,
        dadosHttpVerb.getOrElse("POST", 0).toString,
        dadosHttpVerb.getOrElse("DELETE", 0).toString,
        dadosColunasUrl.mkString(";")
      )
    a
  }

  def aindaNaoSei(sortedEpoch: List[Long], ip: String = "") = {
    val tempList = new ListBuffer[Long]()

    // sessions_qtd, session_sum, sessions_median
    var globalSessionSum = 0L;
    var globalSessionMedian = 0.0;
    var globalTotalSession = 0L;

    for (actualEpoch <- sortedEpoch) {
      if (tempList.isEmpty) {
        tempList += actualEpoch
      } else {
        val beforeTime = tempList.last
        if (actualEpoch - beforeTime < 900L) tempList += actualEpoch
        else {
          val session_sum = if (tempList.size > 1) tempList.sliding(2).map(r => r(1) - r(0)).sum else 0
          val sessions_median = if (tempList.isEmpty) 0L else session_sum / tempList.size
          globalTotalSession += 1
          globalSessionSum += session_sum
          globalSessionMedian += sessions_median
          tempList.clear()
        }
      }
    }

    if (tempList.nonEmpty) {
      val session_sum = if (tempList.size > 1) tempList.sliding(2).map(r => r(1) - r(0)).sum else 0
      val sessions_median = if (tempList.isEmpty) 0.0 else session_sum.toDouble / tempList.size.toDouble
      globalTotalSession += 1
      globalSessionSum += session_sum
      globalSessionMedian += sessions_median
    }
    (globalTotalSession, globalSessionMedian, globalSessionSum)
  }

  def outputFormat(e: Columns): String = {
      s"${e.ip};${e.uReq};" +
      s"${e.ss_time};${e.ss_media};${e.ss_qtd};" +
      s"${e.c100};${e.c200};${e.c300};${e.c400};${e.c500};" +
      s"${e.ttl_bytes};" +
      s"${e.get};${e.put};${e.post};${e.del};"+
      s"${e.columnsUrls}"
  }

  def main(args: Array[String]) {
    val path = if (args.isEmpty) "mini" else "big"
    utilities.setupLogging()
    val conf = new SparkConf().setAppName("VectorLogs").setMaster("local[*]")
    val sc = new SparkContext(conf)


    val knowUrls = sc.textFile(s"./in/$path/know-urls/part-*").map(e => mapUrls(e)).collect().toMap
    val bKnowUlrs = sc.broadcast(knowUrls)


    val input = sc.textFile(s"./in/$path/filter-urls/part-*")
    val logs = input.map(line => NginxLineParser.parse(line))
    val filterData = logs.flatMap(e => if (e.isDefined) Some(generateArrayValues(e.get, bKnowUlrs)) else None)

    val info = filterData.groupBy(e => e.ip)
      .map(e => listaDeObjectos(e._2.toList))
      .map(e => generateVector(e,knowUrls.size ))
      .map(e=>outputFormat(e))

    info.coalesce(1,true).saveAsTextFile(s"./in/$path/vector-params")

    //
    //    val ipUrls: RDD[(String, String)] = logs.flatMap(e => if (e.isDefined) Some((e.get.remoteAddr, e.get.request.URL)) else None)
    //    val uniqueIps = urls.distinct
    //
    //
    //

    sc.stop()
  }


}
