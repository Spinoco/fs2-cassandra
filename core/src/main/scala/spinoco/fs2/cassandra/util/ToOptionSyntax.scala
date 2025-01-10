package spinoco.fs2.cassandra.util

import java.util.Optional

object ToOptionSyntax {
  implicit class OptionalConverter[T](val optional: Optional[T]) extends AnyVal {
    def toOption: Option[T] = if (optional.isPresent) Some(optional.get()) else None
  }
}
