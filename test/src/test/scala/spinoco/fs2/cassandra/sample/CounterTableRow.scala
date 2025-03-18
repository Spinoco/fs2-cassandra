package spinoco.fs2.cassandra.sample

import shapeless.tag.@@
import spinoco.fs2.cassandra.ctype.CType.Counter


case class CounterTableRow(
  intColumn:Int
  , longColumn:Long
  , counterColumn: Long @@ Counter
)
