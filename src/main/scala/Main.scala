import HTTPLogParser._
import java.nio.charset.CodingErrorAction
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.Imports._
import scala.io.Codec._
import scala.io.{Source, Codec, StdIn}
import scala.util.{Success, Failure}
import scala.annotation.tailrec
import scala.concurrent._

object Main {
  val usage = """
    |
    |Options:
    |  --help         produce help message
    |  --uri arg      The connection URI, it must contain a collection
    |                 Example: mongodb://localhost:27017/db.collection
    |  --use arg      spark | mongo""".stripMargin

  val alisDB = "access_log_invariant_search"
  val alcoCO = "access_log_collection"
  case class Options(uri: Option[String] = Some(s"mongodb://localhost:27017/$alisDB.$alcoCO"), use: Option[String] = None)

  val charSet        = "ISO-8859-1" // ISO Latin Alphabet No. 1, a.k.a. ISO-LATIN-1
  implicit val codec = Codec(charSet)
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

  def main(args: Array[String]) : Unit = {

    if (args.length == 0 | args.contains("--help")) {
      Console.err.println(usage)
      sys.exit(1)
    }

    val options = getOptions(getArgs(Map(), args.toList))

    if (options.use.get == "mongo") {
      val mcURI   = MongoClientURI(options.uri.get)
      val mc      = MongoClient(mcURI)
      val db      = mc(mcURI.database.get)
      val co      = db(mcURI.collection.get)
      insertAccessLogs(co, options)
    }
    else
      sys.exit(1)
  }

  private def spin(ftr: Future[_]) {
    while (!ftr.isCompleted) {
      List("!", "/", "$", "\\") foreach { i =>
        Console.err.print(i)
        Thread sleep 200
        Console.err.print("\b")
      }
    }
    Console.err.println("")
  }

  private def insertAccessLogs(co: MongoCollection, options: Options) {
    import NCSACommonLog.toBSONSmall
    def unBuilder = co.initializeUnorderedBulkOperation
    val batchSize = 1000

    def parseInsert(lns: Iterator[String]) = {
      val bldr = unBuilder
      lns.take(batchSize) foreach (
        new parseLine(_).InputLine.run() match {
          case Success(e) => bldr.insert(toBSONSmall(e))
          case _          => // aka Failure
        })
      bldr.execute()
    }

    def batchInsert(filename: String): Unit = {
      val a      = System.currentTimeMillis()
      val lns    = Source.fromFile(filename)(charSet).getLines()
      while (lns.hasNext) parseInsert(lns)
      val z      = System.currentTimeMillis()
      println(s"Inserted ${co.count()} HTTP access logs from $filename in ${a - z} milliseconds.")
    }

    REPL()
    @tailrec
    def REPL(): Unit  = {
      StdIn.readLine("\nenter filename: ") match {
        case ""       => // press enter to exit
        case filename => { batchInsert(filename); REPL() }
      }
    }
  }

  type ArgMap = Map[String, Any]
  private def getArgs(map: ArgMap, args: List[String]): ArgMap = {
    args match {
      case Nil => map
      case "--uri" :: value :: tail =>
        val uriStr: Option[String] =
          if (value == "default") Options().uri
          else Some(value)
        getArgs(map ++ Map("uri" -> uriStr), tail)
      case "--use" :: value :: tail =>
        val useStr: Option[String] =
          if (value == "spark") Some("spark")
          else if (value == "mongo") Some("mongo")
          else None
        getArgs(map ++ Map("use" -> useStr), tail)
      case option :: tail =>
        Console.err.println("Unknown option " + option)
        Console.err.println(usage)
        sys.exit(1)
    }
  }

  private def getOptions(optionMap: Map[String, _]): Options = {
    val default = Options()
    Options(
      uri = optionMap.get("uri") match {
        case None => default.uri
        case Some(v: String) => Some(v)
        //case Some(value: String) => Some(value.asInstanceOf[String])
        //case Some(value) => Some(value.asInstanceOf[String])
      },
      use = optionMap.get("use") match {
        case None => default.use
        case Some(v: String) => Some(v)
      }
    )
  }

  private def performParseComparison(filename: String): Unit = {
    val regex   = false
    val pegPart = true
    val peg     = false

    var goods = 0
    var fails = 0

    println("Regex")
    if (regex) {
      val lines = Source.fromFile(filename)(charSet)
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
      val lines = Source.fromFile(filename)(charSet)
      val then = System.currentTimeMillis()
      lines.getLines() foreach ( line =>
        new parseLine(line.trim).InputLine.run() match {
          case Success(expr: NCSACommonLog) =>
            goods += 1
            if (goods == 1) println(expr)
          case Failure(expr) => fails += 1
        })
      val now = System.currentTimeMillis()
      report(goods, fails, now - then)
    }

    goods = 0
    fails = 0
    println("\nPEG w/ More Accuracy")
    if (peg) {
      val lines = Source.fromFile(filename)(charSet)
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

  private def report(goods: Int, fails: Int, time: Long): Unit = {
    println(s"TOTAL : ${goods+fails}")
    println(s"PARSED: $goods")
    println(s"FAILED: $fails")
    println(s"PERCNT: ${fails.toDouble / (goods + fails)}")
    println(s"TIME  : $time")
  }
}
