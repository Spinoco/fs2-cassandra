package spinoco.fs2.cassandra.util

import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, ColumnDefinition}

import scala.collection.convert.ImplicitConversions.`iterator asScala`

object AsyncResultSetSyntax {
  implicit class AsyncResultSetStageSyntax(val self: AsyncResultSet) extends AnyVal {

    def keys: Set[String] = {
      self.getColumnDefinitions.iterator().toSet.map { a: ColumnDefinition => a.getName.toString }
    }
  }
}
