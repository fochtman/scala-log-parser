import com.mongodb.casbah.Imports._
import org.parboiled2._
import java.util.regex.Pattern
import org.joda.time.format.DateTimeFormat
import com.mongodb.casbah.commons.conversions.scala._
object HTTPLogParser {

  // TODO: update pegFullNCSA with latest knowledge about PEG efficiency

  sealed trait Log
  case class NCSACommonLog (
    host: String,
    rfc: String,
    userName: String,
    date: String,
    method: String,
    resource: String,
    statusCode: String,
    bytes: String,
    id: ObjectId = new ObjectId()
  ) extends Log

  object NCSACommonLog {
    def getRfc(r: String) = if (r == "-") null else r

    def getUsr(u: String) = if (u == "-") null else u

    RegisterJodaTimeConversionHelpers()
    val formatter = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z")

    def getBytes(b: String) = if (b == "-") null else b.toInt

    def toBSON(log: NCSACommonLog): DBObject = {
      MongoDBObject(
        "host"        -> log.host,
        "rfc"         -> getRfc(log.rfc),
        "userName"    -> getUsr(log.userName),
        "date"        -> log.date,
        "method"      -> log.method,
        "resource"    -> log.resource,
        "statusCode"  -> log.statusCode,
        "bytes"       -> getBytes(log.bytes)
      )
    }
    def toBSONSmall(log: NCSACommonLog): DBObject = {
      MongoDBObject(
        "host"        -> log.host,
        "date"        -> formatter.parseDateTime(log.date),
        "method"      -> log.method,
        "resource"    -> log.resource,
        "statusCode"  -> log.statusCode,
        "bytes"       -> getBytes(log.bytes)
      )
    }
  }

  class parseLine(val input: ParserInput) extends Parser {
    import CharPredicate.{Digit, Alpha}
    def InputLine: Rule1[NCSACommonLog] = rule {
      capture(url) ~ ws ~ capture(url) ~ ws ~ capture(url) ~ ws ~
      '[' ~ capture(digit2 ~ '/' ~ alpha3 ~ '/' ~ digit4 ~ ':' ~  // date ../../....:
      digit2 ~ ':' ~ digit2 ~ ':' ~ digit2 ~ ws ~                 // time ..:..:..
      (ch('-') | '+') ~ digit4) ~ ']' ~ ws ~                      // zone (-/+)....
      '"' ~ capture(url ~ ws ~ url) ~ ws ~                        // request type and resource
      capture(vis) ~ '"' ~ ws ~                                   //
      capture(digit3) ~ ws ~                                      // status code
      capture(oneOrMore(Digit)) ~>                                // bytes
        ((h: String, r: String, u: String, dateTime: String, m: String, rq: String, sc: String, b: String) =>
          NCSACommonLog(h, r, u, dateTime, m, rq, sc, b))
    }
    val ws = ' '
    def url    = rule { oneOrMore(!ws ~ ANY) }
    def vis    = rule { oneOrMore(!ws ~ !'"' ~ ANY) }
    def alpha3 = rule { Alpha ~ Alpha ~ Alpha }
    def digit2 = rule { Digit ~ Digit }
    def digit3 = rule { Digit ~ Digit ~ Digit }
    def digit4 = rule { Digit ~ Digit ~ Digit ~ Digit }
  }

  class pegFullNCSA(val input: ParserInput) extends Parser {
    def InputLine: Rule1[NCSACommonLog] = rule {
      Expression ~ EOI
    }

    def Expression: Rule1[NCSACommonLog] = rule {
      (Host ~ Rfc ~ UserName ~ Date ~ Method ~ Request ~ StatusCode ~ Bytes) ~>
        ((h: String, r: String, u: String, dmy: String, hms: String, zone: String, m: String, rq: String, sc: String, b: String) =>
          NCSACommonLog(h, r, u, dmy+hms+zone, m, rq, sc, b))
    }

    /** Host, Start **/
    def Host      = rule { capture(oneOrMore(URLChars)) ~ ws }
    def Rfc       = rule { capture('-') ~ ws }
    def UserName  = rule { capture('-') ~ ws }

    /** Date, Start **/
    def Date      = rule { '[' ~ DMY ~ HMS ~ Zone ~ ']' ~ ws }
    def DMY       = rule { capture(Day ~ '/' ~ Month ~ '/' ~ Year) }
    def Day       = rule { twoDigits }
    def Month     = rule { (1 to 3).times(CharPredicate.Alpha) }
    def Year      = rule { fourDigits }
    def HMS       = rule { capture(':' ~ Hour ~ ':' ~ Minute ~ ':' ~ Second) ~ ws }
    def Hour      = rule { twoDigits }
    def Minute    = rule { twoDigits }
    def Second    = rule { twoDigits }
    def Zone      = rule { capture((ch('+') | '-') ~ fourDigits) }
    /** Date, End **/

    /** HTTP Request, Start **/
    def Request    = rule { Resource ~ ws }
    def Method     = rule { '"' ~ capture("GET" | "POST" | "HEAD" | "PUT" | "DELETE" | "TRACE" | "CONNECT") ~ ws }
    def Resource   = rule { capture(ResType0) ~ '"' }
    def ResType0   = rule { oneOrMore(oneOrMore(URLChars)).separatedBy(oneOrMore(ws)) }

    def StatusCode = rule { capture((ch('1') | ch('2') | ch('3') | ch('4') | '5') ~ twoDigits) ~ ws }
    def Bytes      = rule { capture('-' | oneOrMore(Digits)) }

    def ws         = rule { CharPredicate(' ') }
    def Digits     = rule { CharPredicate.Digit }
    def URLChars   = rule { CharPredicate.Visible -- '"' }
    def AlphaNums  = rule { CharPredicate.AlphaNum }
    def twoDigits  = rule { (1 to 2).times(Digits) }
    def fourDigits = rule { (1 to 4).times(Digits) }

    //def Protocol   = rule { optional('/') ~ oneOrMore(ws) ~ capture("HTTP/" ~ ("1.0" | "V1.0")) ~ '"' | capture('"') | ws ~ capture('"') }
    /** HTTP Request, End **/
  }

  /**
   * Taken from an Apache Spark example
   */
                          // 1:IP  2:client 3:user 4:date time                   5:method 6:req 7:proto   8:respcode 9:size
  val LOG_ENTRY_PATTERN = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+ \\S+) (\\S+)\" (\\d{3}) (\\d+)"
  val PATTERN = Pattern.compile(LOG_ENTRY_PATTERN)
  def regexNCSA(input: String): Option[NCSACommonLog] = {
    val m = PATTERN.matcher(input)
    if (!m.find())
      None
    else
      Option(NCSACommonLog(m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8)))
  }
}

// DateTime(int year, int monthOfYear, int dayOfMonth, int hourOfDay, int minuteOfHour)
// 01/Jul/1995:00:00:06 -0400
// val dtf = ISODateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") .withLocale(Locale.ROOT)	.withChronology(ISOChronology.getInstanceUTC());
// import org.joda.time.DateTimeComparator._
// val d2 = new DateTime(1995, 7, 1, 5, 6, 7).withZone(DateTimeZone.forID("-04:00"))
