package spinoco.fs2.cassandra.util

import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, ResultSet, Row}

import scala.collection.convert.ImplicitConversions.`iterator asScala`

object DrainSyntax {

  implicit class ResultSetDrainSyntax(val self:ResultSet) extends AnyVal {
    /** Given a ResultSet, retrieve all results that are fetched at the moment. */
    def drain:Vector[Row] = {
      val count = self.getAvailableWithoutFetching
      spinoco.fs2.cassandra.util.iterateN(self.iterator(), count)
    }
  }

  implicit class AsyncResultSetDrainSyntax(val self: AsyncResultSet) extends AnyVal {
    /** Given an AsyncResultSet, retrieve all results that are fetched at the moment. */
    def drain:Vector[Row] = {
      self.currentPage().iterator.toVector
    }
  }
}
