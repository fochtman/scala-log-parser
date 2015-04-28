
import HTTPLogParser._
import java.nio.charset.CodingErrorAction
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.Imports._
import io.Codec._
import scala.io.{Source, Codec, StdIn}
import scala.util.{Success, Failure}
import scala.annotation.tailrec
import scala.concurrent._
import java.io.{ByteArrayOutputStream, PrintStream}
import scala.Some
import scala.collection.JavaConverters._
import com.mongodb.util.JSON


object Main {
  val usage = """
    |
    |Example:
    |  mongoimport --uri mongodb://localhost/my_db.my_collection
    |
    |Options:
    |  --help                                produce help message
    |  --uri arg                             The connection URI - must contain a collection
    |                                        mongodb://[username:password@]host1[:port1][,host2[:port2]]/database.collection[?options]""".stripMargin

  val alisDB = "access_log_invariant_search"
  val alco   = "access_log_collection"
  case class Options(uri: Option[String] = Some(s"mongodb://localhost:27017/$alisDB.$alco"))

  val charSet        = "ISO-8859-1" // ISO Latin Alphabet No. 1, a.k.a. ISO-LATIN-1
  implicit val codec = Codec(charSet)
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)


  def main(args: Array[String]) : Unit = {

    if (args.length == 0 | args.contains("--help")) {
      Console.err.println(usage)
      sys.exit(1)
    }

    val optionMap = parseArgs(Map(), args.toList)
    val options = getOptions(optionMap)

    val mongoClientURI = MongoClientURI(options.uri.get)

    println(mongoClientURI)
    println(mongoClientURI.database.get)
    println(mongoClientURI.collection.get)

    val mc = MongoClient(mongoClientURI)
    val db = mc(mongoClientURI.database.get)
    val co = db(mongoClientURI.collection.get)
    importAccessLogs(co, options)
  }

  private def parseArgs(map: Map[String, Any], args: List[String]): Map[String, Any] = {
    args match {
      case Nil => map
      //case "--uri" :: value :: tail =>
      case "--uri" :: value =>
        val uriStr: Option[String] =
          if (value.head == "default") Options().uri
          else Some(value.head)
        parseArgs(map ++ Map("uri" -> uriStr), Nil)
      case option :: tail =>
        Console.err.println("Unknown option " + option)
        Console.err.println(usage)
        sys.exit(1)
    }
  }

  private def getOptions(optionMap: Map[String, _]): Options = {
    /*
    val default = Options()
    Options(
      uri = optionMap.get("uri") match {
        case None => default.uri
        case Some(value: String) => Some(value.asInstanceOf[String])
        //case Some(value) => Some(value.asInstanceOf[String])
      }
    )
    */
    Options()
  }

  private def spinWheel(someFuture: Future[_]) {
    // Let the user know something is happening until futureOutput isCompleted
    val spinChars = List("!", "/", "$", "\\")
    while (!someFuture.isCompleted) {
      spinChars.foreach({
        case char =>
          Console.err.print(char)
          Thread sleep 200
          Console.err.print("\b")
      })
    }
    Console.err.println("")
  }

  private def importAccessLogs(co: MongoCollection, options: Options) {
    def unBuilder = co.initializeUnorderedBulkOperation
    val batchSize = 1000

    def batchInsert(filename: String): Unit = {
      val a      = System.currentTimeMillis()
      val lns    = Source.fromFile(filename)(charSet).getLines()
      while (lns.hasNext) {
        val bldr = unBuilder
        lns.take(batchSize) foreach (line =>
          new pegPartNCSA(line.trim).InputLine.run() match {
            case Success(e) => bldr.insert(NCSACommonLog.toBSONSmall(e))
            case _ => // aka Failure
          })
        bldr.execute()
      }
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
        new pegPartNCSA(line.trim).InputLine.run() match {
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
