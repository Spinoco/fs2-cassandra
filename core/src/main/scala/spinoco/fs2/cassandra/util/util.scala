package spinoco.fs2.cassandra


import scala.annotation.tailrec

/**
  * Created by pach on 04/06/16.
  */
package object util {

  def Try[A](f: => A):Either[Throwable,A] =
    try { Right(f) } catch { case t: Throwable => Left(t) }

  /**
    * Iterate through supplied iterator, but only collect up to `count` elements in iterator
    */
  def iterateN[A](it:java.util.Iterator[A], count:Int):Vector[A] = {
    @tailrec
    def go(acc:Vector[A],rem:Int):Vector[A] = {
      if (rem == 0 || ! it.hasNext) acc
      else  go(acc :+ it.next(), rem - 1)
    }
    go(Vector.empty,count)
  }

  /** replaces in rpepared statement the name palceholders with CQL form values **/
  def replaceInCql(cql:String, values:Map[String,String]):String = {
    @tailrec
    def go(pos:Int, acc:String):String = {
      val start = cql.indexOf(':', pos)
      if (start < 0 || start >= cql.length) acc + cql.substring(pos)
      else {
        val end =
          cql.indexWhere(ch => !(ch.isLetterOrDigit || ch == '_'), start+1) match {
            case idx if idx < 0 => cql.length - 1
            case idx => idx
          }

        val key = cql.substring(start+1,end).trim
        val value =
        values.get(key) match {
          case None => s":"+key
          case Some(v) => v
        }
        go(end,acc + cql.substring(pos, start) + value + " ")
      }
    }

    go(0,"")
  }

  implicit class ThrowableRethrowSyntax(self: Throwable){
    def rethrow(format: String, str: String*): Throwable = {
      val msg = String.format(format, str:_*)
      new Throwable(msg + s"\n    got: ${self.getMessage}")
    }

    def rethrowStmtInfo[A,B](row: A, stmt: B): Throwable = {
      self.rethrow(s"At row '%s'\n    in statement: '%s'", row.toString, stmt.toString)
    }
  }
}
