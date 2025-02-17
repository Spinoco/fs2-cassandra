package spinoco.fs2.cassandra.util

import cats.effect.{Async, ContextShift}
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, Row}
import fs2.Stream
import spinoco.fs2.cassandra.util.AsyncResultSetSyntax.AsyncResultSetSyntaxes

import java.util.concurrent.CompletionStage
import java.util.function.BiConsumer

object CompletionStageSyntax {


  implicit class CompletionStageSyntaxes[A](val self: CompletionStage[A]) extends AnyVal {
    /**
      * Converts the `CompletionStage` to an `F`.
      * Note that hence the `self` exists already, the completion stage (or future that is backed by this) is
      * likely already running. This is just a way to convert it to `F` effect.
      *
      * If you want to assure that the `CompletionStage` is run only when `F` is run, wrap resulting code in `Sync[F].suspend`
      *
      * @tparam F
      * @return
      */
    def toF[F[_] : Async] : F[A] = CompletionStageSyntax.completionStageToFUnsafe(self)
  }

  /**
    * Converts a `CompletionStage` to an `F`.
    * Note that cs is passed as reference, the completion stage (or future that is backed by this) is
    * likely already running. This is just a way to convert it to `F` effect.
    *
    * @param cs Compelting stage
    */
  def completionStageToFUnsafe[F[_]
    : Async
    , A](cs: CompletionStage[A]): F[A] = {
    Async[F].async { cb =>
      cs.whenComplete(new BiConsumer[A, Throwable] {
        def accept(a: A, t: Throwable): Unit = {
          if (a != null) cb(Right(a))
          else if (t != null) cb(Left(t))
          else cb(Left(new RuntimeException("CompletionStage returned null for both value and error")))
        }
      })
      (); // ignore the result of whenComplete
    }
  }

  implicit class AsyncResultSetStreamSyntax(val self: CompletionStage[AsyncResultSet]) extends AnyVal {
    def toStream[F[_] : Async] : Stream[F, Row] = {
      val asyncResultSet: F[AsyncResultSet] = CompletionStageSyntax.completionStageToFUnsafe(self)
      Stream.eval(asyncResultSet).flatMap(_.toStream)
    }
  }

}
