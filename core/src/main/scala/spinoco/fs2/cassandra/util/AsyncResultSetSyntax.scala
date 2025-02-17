package spinoco.fs2.cassandra.util

import cats.effect.Async
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, ColumnDefinition, Row}
import fs2.Stream
import spinoco.fs2.cassandra.util.CompletionStageSyntax.CompletionStageSyntaxes

import java.util.concurrent.CompletionStage
import scala.collection.convert.ImplicitConversions.`iterator asScala`

object AsyncResultSetSyntax {
  implicit class AsyncResultSetSyntaxes(val self: AsyncResultSet) extends AnyVal {
    /** Given an AsyncResultSet, get the set of column names as strings. */
    def keys: Set[String] = {
      self.getColumnDefinitions.iterator().toSet.map { a: ColumnDefinition => a.getName.toString }
    }
    /** Given an AsyncResultSet, retrieve all results that are fetched at the moment. */
    def drain:Vector[Row] = {
      self.currentPage().iterator.toVector
    }

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

}
