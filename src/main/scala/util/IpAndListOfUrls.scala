package dev.vergil
package util

object IpAndListOfUrls {
  val ws = "\\s+"
  val remote_addr = "([^-]*)" + ws
  val remote_user = "-\\s+(\\S+)" + ws
  val time_local = "(\\[.+?\\])" + ws
  val request = "\"(.+?)\"" + ws
  val status = "(\\S+)" + ws
  val bytes_sent = "(\\S+)" + ws
  val referer = "\"(.+?)\"" + ws
  val agent = "\"(.*?)\""

  private val regex_definition = s"$remote_addr$remote_user$time_local$request$status$bytes_sent$referer$agent"
  private val regex = regex_definition.r

}
