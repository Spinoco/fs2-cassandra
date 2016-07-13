package spinoco.fs2.cassandra.sample

import java.time.LocalDateTime
import java.util.UUID

import shapeless.tag._
import spinoco.fs2.cassandra.CType.{Ascii, Type1}

case class TupleTableRow(
  intColumn: Int
  , longColumn: Long
  , tuple2Column: (String, Int)
  , tuple3Column: (String, String @@ Ascii, Long)
  , tuple4Column: (String, String @@ Ascii, UUID, UUID @@ Type1)
  , tuple5Column: (String, String @@ Ascii, UUID, UUID @@ Type1, LocalDateTime)
)
