package dev.vergil
package util

case class FilterObject (
                           ip: String,
                           epoch: Long,
                           httpVerb: String,
                           indexPrune: Int,
                           status: Int,
                           bytes: Long,
                         )

case class ListFilterObject (
                          ip: String,
                          listEpoch: List[Long],
                          httpVerb: List[String],
                          indexPrune: List[Int],
                          status: List[Int],
                          bytes: List[Long],
                        )
case class Columns (
                   ip: String,
                   uReq: String,
                   ss_time: String,
                   ss_media: String,
                   ss_qtd: String,
                   c100: String,
                   c200: String,
                   c300: String,
                   c400: String,
                   c500: String,
                   ttl_bytes: String,
                   get: String,
                   put: String,
                   post: String,
                   del: String,
                   columnsUrls: String,

                   )