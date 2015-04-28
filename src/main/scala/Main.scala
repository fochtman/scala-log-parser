
import java.nio.charset.CodingErrorAction
import com.mongodb.casbah.MongoClient

import scala.io.Codec

import HTTPLogParser._
import util.{Success, Failure}
import io.Codec._

object Main {

  val charSet        = "ISO-8859-1" // ISO Latin Alphabet No. 1, a.k.a. ISO-LATIN-1
  implicit val codec = Codec(charSet)
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

  def main(args: Array[String]) : Unit = {
    val filename       = args(0)
    val lines = scala.io.Source.fromFile(filename)(charSet)

    performParseComparison(filename)

    val mongo = false

    if (mongo){
      val mongoClient = MongoClient("localhost", 27017)
      val db          = mongoClient("test-logfiles")
      val coll        = db("test-coll")
      coll.drop()
      lines.getLines() foreach ( line =>
        new pegFullNCSA(line.trim).InputLine.run() match {
          case Success(expr: NCSACommonLog) =>
            coll.insert(NCSACommonLog.toBson(expr))
          case Failure(expr) =>
        }
      )
      println(coll.count())
    }
  }

  def performParseComparison(filename: String): Unit = {
    val regex   = true
    val pegPart = true
    val peg     = true

    var goods = 0
    var fails = 0

    println("Regex")
    if (regex) {
      val lines = scala.io.Source.fromFile(filename)(charSet)
      val then = System.currentTimeMillis()
      lines.getLines() foreach ( line =>
        regexNCSA(line.trim) match {
          case Some(expr: NCSACommonLog) => goods += 1
          case None => fails += 1
        })
      val now = System.currentTimeMillis()
      report(goods, fails, now - then)
    }

    goods = 0
    fails = 0
    println("\nPEG w/ Regex Accuracy")
    if (pegPart) {
      val lines = scala.io.Source.fromFile(filename)(charSet)
      val then = System.currentTimeMillis()
      lines.getLines() foreach ( line =>
        new pegPartNCSA(line.trim).InputLine.run() match {
          case Success(expr: NCSACommonLog) => goods += 1
          case Failure(expr) => fails += 1
        })
      val now = System.currentTimeMillis()
      report(goods, fails, now - then)
    }

    goods = 0
    fails = 0
    println("\nPEG w/ More Accuracy")
    if (peg) {
      val lines = scala.io.Source.fromFile(filename)(charSet)
      val then = System.currentTimeMillis()
      lines.getLines() foreach ( line =>
        new pegFullNCSA(line.trim).InputLine.run() match {
          case Success(expr: NCSACommonLog) => goods += 1
          case Failure(expr) => fails += 1
        })
      val now = System.currentTimeMillis()
      report(goods, fails, now - then)
    }
  }

  def report(goods: Int, fails: Int, time: Long): Unit = {
    println(s"TOTAL : ${goods+fails}")
    println(s"PARSED: $goods")
    println(s"FAILED: $fails")
    println(s"PERCNT: ${fails.toDouble / (goods + fails)}")
    println(s"TIME  : $time")
  }
}
