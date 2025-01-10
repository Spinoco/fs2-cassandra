package spinoco.fs2.cassandra.util

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata
import spinoco.fs2.cassandra.util.ToOptionSyntax.OptionalConverter

object KeyspaceSyntax {
  implicit class GetKeyspaceMetadataSyntax(val self: CqlSession) extends AnyVal {
    def getKeyspaceMetadata(keyspaceName: String): Option[KeyspaceMetadata] = {
      self
        .getMetadata
        .getKeyspace(keyspaceName)
        .toOption
    }
  }
}
