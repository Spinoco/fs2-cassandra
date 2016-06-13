package spinoco.fs2.cassandra


import scala.annotation.tailrec

/**
  * Created by pach on 04/06/16.
  */
package object util {

  def Try[A](f: => A):Either[Throwable,A] =
    try { Right(f) } catch { case t: Throwable => Left(t) }


  def iterateN[A](it:java.util.Iterator[A], count:Int):Vector[A] = {
    @tailrec
    def go(acc:Vector[A],rem:Int):Vector[A] = {
      if (it.hasNext && rem > 0) go(acc :+ it.next(), rem - 1)
      else acc
    }
    go(Vector.empty,count)
  }

}
