import org.parboiled2._

object HTTPLogParser {

  sealed trait Log
  case class Host(s: String) extends Log
  case class Rfc(s: String) extends Log
  case class UserName(s: String) extends Log
  case class Date(dmy: String, hms: String, zone: String) extends Log
  case class Request(m: String, r: String, p: String) extends Log
  case class Request0(m: String, r: String) extends Log
  case class StatusCode(s: String) extends Log
  case class Bytes(s: String) extends Log

  class NCSA(val input: ParserInput) extends Parser {
    def InputLine = rule {
      Expression ~ EOI
    }

    def Expression: Rule1[List[Log]] = rule {
      host ~ rfc ~ userName ~ date ~ request ~ statusCode ~ bytes ~> (List(_: Host, _: Rfc, _: UserName, _: Date, _: Request0, _: StatusCode, _: Bytes))
      //host ~ rfc ~ userName ~ date ~ request ~ statusCode ~ bytes ~> ((h: Host, r: Rfc, u: UserName, d: Date, rq: Request, sc: StatusCode, b: Bytes) => List(h, r, u, d, rq, sc, b))
    }

    /** Host, Start **/
    def host: Rule1[Host]        = rule { SubDomain ~> (Host(_)) ~ ws }
    //def host: Rule1[Host]        = rule { (IP | SubDomain) ~> (Host(_)) ~ ws }
    def IP: Rule1[String]        = rule { capture(oneOrMore(oneOrMore(Digits)).separatedBy('.')) }
    def SubDomain: Rule1[String] = rule { capture(oneOrMore(URLChars)) }
    //def SubDomain: Rule1[String] = rule { capture(oneOrMore(oneOrMore(AlphaNums).separatedBy(anyOf(".-_")))) }

    def rfc: Rule1[Rfc]            = rule { capture('-') ~> (Rfc(_)) ~ ws }
    def userName: Rule1[UserName]  = rule { capture('-') ~> (UserName(_)) ~ ws }

    /** Date, Start **/
    def date      = rule { '[' ~ (DMY ~ HMS ~ Zone ~> (Date(_, _, _))) ~ ']' ~ ws }
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
    def request    = rule { '"' ~ (Method ~ Resource ~> (Request0(_, _))) ~ ws } //~ '"' ~ ws }
    //def request    = rule { '"' ~ (Method ~ Resource ~ Protocol ~> (Request(_, _, _))) ~ ws } //~ '"' ~ ws }
    def Method     = rule { capture("GET" | "POST" | "HEAD" | "PUT" | "DELETE" | "TRACE" | "CONNECT") ~ ws }
    //def Resource   = rule { capture(oneOrMore(URLChars)) ~ '"' }
    //def Protocol   = rule { optional('/') ~ oneOrMore(ws) ~ capture("HTTP/" ~ ("1.0" | "V1.0")) ~ '"' | capture('"') | ws ~ capture('"') }
    def Resource   = rule { capture(oneOrMore(oneOrMore(URLChars)).separatedBy(oneOrMore(ws)) | oneOrMore(URLChars) ~ ws) ~ '"' }
    def Protocol   = rule { optional('/') ~ oneOrMore(ws) ~ capture("HTTP/" ~ ("1.0" | "V1.0")) ~ '"' | capture('"') | ws ~ capture('"') }
    /** HTTP Request, End **/

    def statusCode = rule { capture((ch('1') | ch('2') | ch('3') | ch('4') | '5') ~ twoDigits) ~> (StatusCode(_)) ~ ws }
    def bytes      = rule { capture('-' | oneOrMore(Digits)) ~> (Bytes(_)) }

    def ws          = rule { CharPredicate(' ') }
    def Digits     = rule { CharPredicate.Digit }
    def URLChars   = rule { CharPredicate.Visible -- '"' }
    def AlphaNums  = rule { CharPredicate.AlphaNum }
    def twoDigits  = rule { (1 to 2).times(Digits) }
    def fourDigits = rule { (1 to 4).times(Digits) }
  }
}
