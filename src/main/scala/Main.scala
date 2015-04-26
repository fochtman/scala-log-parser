
import HTTPLogParser._
import util.{Success, Failure}

object Main {

  def main(args: Array[String]) : Unit = {
    var i = 1
    val filename = args(0)
    val finalResult =
      for {
        line <- scala.io.Source.fromFile(filename).getLines()
        res = new NCSA(line).InputLine.run()
      } yield {

        res match {
          case Success(expr) =>
            i += 1
            true
          case Failure(expr) =>
            println(i, "     ", expr)
            i += 1
            false
        }
      }
    println("FINISHED PARSING")
    println("# PARSED CORRECTLY:")
    println(finalResult.toList.count(_ == true))
    println
    println("OUT OF TOTAL:")
    println(i)

  }

}
