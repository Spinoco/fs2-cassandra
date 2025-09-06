package spinoco.fs2.cassandra.internal

import cats.effect.Async
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, ColumnDefinition, Row}
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata
import fs2.Stream

import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.BiConsumer
import scala.collection.JavaConverters._

object Util {

  /**
    * Converts a `CompletionStage` to an `F`.
    * Note that cs is lazily passed allowing it to be run when resulting [F] is run
    *
    * @param cs Compelting stage
    */
  def evalCS[F[_]
    : Async
    , A](cs: => CompletionStage[A]): F[A] = {
    Async[F].async_[A] { cb =>
      cs.whenComplete(new BiConsumer[A, Throwable] {
        def accept(a: A, t: Throwable): Unit = {
          if (a != null) cb(Right(a))
          else if (t != null) cb(Left(t))
          else cb(Left(new RuntimeException("CompletionStage returned null for both value and error")))
        }
      })
      () // ignore the result of whenComplete
    }
  }

  /** converts Java Optional to Scala Option */
  def toOption[T](optional: Optional[T]): Option[T] = {
    if (optional.isPresent) Some(optional.get()) else None
  }

  /** gets keyspace metadata from a CqlSession */
  def getKeyspaceMetadata(cqlSession: CqlSession, keyspaceName: String): Option[KeyspaceMetadata] = {
    toOption(cqlSession.getMetadata.getKeyspace(keyspaceName))
  }

  /** gets the set of column names as strings from a Row */
  def keys(row: Row): Set[String] = {
    row.getColumnDefinitions.iterator().asScala.toSet.map { a: ColumnDefinition => a.getName.toString }
  }

  /** drains current page of the resultset  */
  def drainCurrentPage(rs: AsyncResultSet): Vector[Row] = {
    rs.currentPage().asScala.toVector
  }

  /** converts resultset to stream that fetches next pages as the stream is evaluated */
  def asStream[F[_] : Async](rs: AsyncResultSet): Stream[F, Row] = {
    def go(drained: AsyncResultSet): Stream[F, Row] = {
      if (!drained.hasMorePages) {
        Stream.empty
      } else {
        Stream.eval(evalCS(drained.fetchNextPage)).flatMap { rs =>
          Stream.emits(drainCurrentPage(rs)) ++ go(rs)
        }
      }
    }

    Stream.emits(drainCurrentPage(rs)) ++ go(rs)
  }



}
