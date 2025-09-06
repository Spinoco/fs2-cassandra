package spinoco.fs2.cassandra.util

import com.datastax.oss.driver.api.core.cql.{ColumnDefinition, Row}

import scala.collection.convert.ImplicitConversions.`iterator asScala`

object RowSyntax {
  implicit class RowKeySyntax(val self: Row) extends AnyVal {
    /** Given an AsyncResultSet, get the set of column names as strings. */
    def keys: Set[String] = {
      self.getColumnDefinitions.iterator().toSet.map { a: ColumnDefinition => a.getName.toString }
    }
  }
}
