package dev.vergil
package util

/*
  Nginx log format http://nginx.org/en/docs/http/ngx_http_log_module.html
  $remote_addr - $remote_user [$time_local] "$request" $status $bytes_sent "$http_referer" "$http_user_agent" "$gzip_ratio"
 */
case class NginxLogRecord(
                           remoteAddr: String, // should be an ip address, but may also be the hostname if hostname-lookups are enabled
                           remoteUser: String, // typically '-'
                           timeLocal: String, // [day/month/year:hour:minute:second zone]
                           request: Request,
                           status: String, // HTTP Status,
                           receivedBytes: String, // Bytes received in the response
                           URLReferer: String, // Referer URL
                           UserAgent: String, // Which User Agent
                         )

case class Request(
                    verb: String = "", // HTTP verb GET, POST, etc
                    URL: String = "", // Resource accessed (URL)
                    HTTPVersion: String = "" // HTTP version: 1.1, 1.0
                  )
