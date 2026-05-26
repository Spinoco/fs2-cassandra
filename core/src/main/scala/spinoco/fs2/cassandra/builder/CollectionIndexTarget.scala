package spinoco.fs2.cassandra.builder

sealed trait CollectionIndexTarget {
  def wrap(field: String): String
}

object CollectionIndexTarget {
  case object Keys    extends CollectionIndexTarget { def wrap(f: String): String = s"KEYS($f)" }
  case object Values  extends CollectionIndexTarget { def wrap(f: String): String = s"VALUES($f)" }
  case object Entries extends CollectionIndexTarget { def wrap(f: String): String = s"ENTRIES($f)" }
  case object Full    extends CollectionIndexTarget { def wrap(f: String): String = s"FULL($f)" }
}
