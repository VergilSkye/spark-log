package dev.vergil
package main

object MainObject {
  val usage =
    """
      Usage: RegexRecon <options> input
      [-b <bin_size>]
      [-w <wanted_w>]
      [-u <unwated_w>]
      <input(s)> Input files
      [-b] The value of the number of the bins that divides de data in the histogram.
      [-w] Wanted words: Seek URL whose addresses contains desired words, separeted by comma, no space.
      [-u] Unwanted words: Words that are banned from processing, even if they are together with wanted words, separeted by comma, no space.
      """

  def main(args: Array[String]): Unit = {
    if (args.length == 0) println(usage)
    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]


    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s : String) = (s(0) == '-')
      list match {
        case Nil => map
        case "-b" :: value :: tail =>
          nextOption(map ++ Map('b -> value.toInt), tail)
        case "-w" :: value :: tail =>
          nextOption(map ++ Map('w -> value.toString), tail)
        case "-u" :: value :: tail =>
          nextOption(map ++ Map('u -> value.split(",").toList), tail)
        case string :: opt2 :: tail if isSwitch(opt2) =>
          nextOption(map ++ Map('infile -> string), list.tail)
        case string :: Nil =>  nextOption(map ++ Map('infile -> string), list.tail)
        case option :: tail => println("Unknown option "+option)
          sys.exit(1)
      }
    }
    val options = nextOption(Map(),arglist)
    println(options)
  }
}
