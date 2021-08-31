package dev.vergil
package util

import scala.util.matching.Regex

object NginxLineParser extends Serializable {
  val ws = "\\s+"
  val remote_addr = "([^-]*)" + ws
  val remote_user = "-\\s+(\\S+)" + ws
  val time_local = "(\\[.+?\\])" + ws
  val request = "\"(.*?)\"" + ws
  val status = "(\\S+)" + ws
  val bytes_sent = "(\\S+)" + ws
  val referer = "\"(.+?)\"" + ws
  val agent = "\"(.*?)\""

  private val regex_definition = s"$remote_addr$remote_user$time_local$request$status$bytes_sent$referer$agent"
  val regex: Regex = regex_definition.r

  /**
   * @param record Assumed to be an Nginx access log.
   * @return An dev.vergilskye.tcc.util.NginxLogRecord instance wrapped in an Option.
   */
  def parse(record: String): Option[NginxLogRecord] = {
    def parseRequestField(request: String): Option[(String, String, String)] = {
      request.split(" ").toList match {
        case List(a, b, c) => Some((a, b, c))
        case other => None
      }
    }

    record match {
      case regex(ip, client, datetime, req, status, bytes, referer, agent) =>
        val requestTuple = parseRequestField(req)
        Some(
          NginxLogRecord(
            ip,
            client,
            datetime.replaceAll("\\[", "").replaceAll("\\]","").split("\\+")(0).replaceAll("\\s+","") ,
            if (requestTuple.isDefined) Request(requestTuple.get._1  , requestTuple.get._2 , requestTuple.get._3) else Request(),
            status,
            bytes,
            referer,
            agent
          )
        )
      case _ => None
    }
  }
}
