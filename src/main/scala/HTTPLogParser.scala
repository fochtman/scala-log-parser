import org.parboiled2._

object HTTPLogParser {

  sealed trait Log
  case class NCSACommonLog (
    host: String,
    rfc: String,
    userName: String,
    date: String,
    method: String,
    resource: String,
    statusCode: String,
    bytes: String
  ) extends Log

  class NCSA(val input: ParserInput) extends Parser {
    def InputLine = rule {
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

    def ws          = rule { CharPredicate(' ') }
    def Digits     = rule { CharPredicate.Digit }
    def URLChars   = rule { CharPredicate.Visible -- '"' }
    def AlphaNums  = rule { CharPredicate.AlphaNum }
    def twoDigits  = rule { (1 to 2).times(Digits) }
    def fourDigits = rule { (1 to 4).times(Digits) }

    //def Protocol   = rule { optional('/') ~ oneOrMore(ws) ~ capture("HTTP/" ~ ("1.0" | "V1.0")) ~ '"' | capture('"') | ws ~ capture('"') }
    /** HTTP Request, End **/
  }
}
