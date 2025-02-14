package spinoco.fs2.cassandra.util

import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, Row}

import scala.collection.convert.ImplicitConversions.`iterator asScala`

object DrainSyntax {
  implicit class AsyncResultSetDrainSyntax(val self: AsyncResultSet) extends AnyVal {
    /** Given an AsyncResultSet, retrieve all results that are fetched at the moment. */
    def drain:Vector[Row] = {
      self.currentPage().iterator.toVector
    }
  }
}
