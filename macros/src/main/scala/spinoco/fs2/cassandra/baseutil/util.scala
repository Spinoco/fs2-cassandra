package spinoco.fs2.cassandra

import scodec.{Attempt, Err}

import scala.annotation.tailrec
import scala.util.Try

/**
  * Created by pach on 04/06/16.
  */
package object baseutil {

  /** converts `f` to attempt. If `f` throws returns Failure, success otherwise */
  def attempt[A](f: => A): Attempt[A] = {
    Try(f).fold(
      e => Attempt.failure(Err(e.getMessage)),
      a => Attempt.successful(a)
    )
  }

  /** replaces in prepared statement the name placeholders with CQL form values **/
  def replaceInCql(cql: String, values: Map[String, String]): String = {
    @tailrec
    def go(pos: Int, acc: String): String = {
      val start = cql.indexOf(':', pos)
      if (start < 0 || start >= cql.length) acc + cql.substring(pos)
      else {
        val end =
          cql.indexWhere(ch => !(ch.isLetterOrDigit || ch == '_'), start+1) match {
            case idx if idx < 0 => cql.length
            case idx => idx
          }

        val key = cql.substring(start + 1, end).trim
        val value =
        values.get(key) match {
          case None => s":" + key
          case Some(v) => v
        }
        go(end, acc + cql.substring(pos, start) + value)
      }
    }

    go(0, "")
  }

  object AnnotatedException {
    def withStmt(err: Throwable, stmt: String): Throwable = {
      new Throwable(s"In statement: '$stmt'", err)
    }

    def withField(err: Throwable, field: String): Throwable = {
      new Throwable(s"At field: '$field'", err)
    }
  }

}
