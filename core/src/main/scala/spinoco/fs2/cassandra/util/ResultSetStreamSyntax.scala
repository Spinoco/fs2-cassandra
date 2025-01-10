package spinoco.fs2.cassandra.util

import cats.effect.Async
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, Row}
import fs2.Stream
import spinoco.fs2.cassandra.util.DrainSyntax.AsyncResultSetDrainSyntax
import spinoco.fs2.cassandra.util.concurrent.CompletionStageSyntax

import java.util.concurrent.CompletionStage

object ResultSetStreamSyntax {
  implicit class AsyncResultSetToStreamSyntax(val self: AsyncResultSet) extends AnyVal {
    def toStream[F[_] : Async] : Stream[F, Row] = {
      def go(drained: AsyncResultSet):Stream[F,Row] = {
        if (!drained.hasMorePages) {
          Stream.empty
        } else {
          Stream.eval(drained.fetchNextPage.toF).flatMap { rs =>
            Stream.emits(rs.drain) ++ go(rs)
          }
        }
      }
      Stream.emits(self.drain) ++ go(self)
    }
  }

  implicit class CompletionStageStreamSyntax(val self: CompletionStage[AsyncResultSet]) extends AnyVal {
    def toStream[F[_] : Async] : Stream[F, Row] = {
      val asyncResultSet: F[AsyncResultSet] = concurrent.completionStageToFUnsafe(self)
      Stream.eval(asyncResultSet).flatMap(_.toStream)
    }
  }
}
