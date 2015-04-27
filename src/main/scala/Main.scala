
import java.nio.charset.CodingErrorAction
import scala.io.Codec

import HTTPLogParser._
import util.{Success, Failure}
import io.Codec._

object Main {

  def main(args: Array[String]) : Unit = {
    val filename = args(0)

    // ISO Latin Alphabet No. 1, a.k.a. ISO-LATIN-1
    val charSet = "ISO-8859-1"
    implicit val codec = Codec(charSet)
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var i = 1
    val lines = scala.io.Source.fromFile(filename)(charSet)

    val finalResult =
      for {
        l <- lines.getLines()
        res = new NCSA(l.trim).InputLine.run()
      } yield {
        res match {
          case Success(expr) =>
            i += 1
            1.toByte
          case Failure(expr) =>
            i += 1
            0.toByte
        }
      }
    println("FINISHED PARSING")

    val count = finalResult.count(_ == 1.toByte)

    println("# PARSED CORRECTLY: " + count)
    println("OUT OF TOTAL: " + i)
    println
    println(i - count)
  }
}
