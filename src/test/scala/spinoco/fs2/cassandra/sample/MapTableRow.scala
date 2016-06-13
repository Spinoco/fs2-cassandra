package spinoco.fs2.cassandra.sample

case class MapTableRow(
  intColumn: Int
  , longColumn: Long
  , mapStringColumn: Map[String,String]
  , mapIntColumn: Map[Int,String]
)


object MapTableRow {
  val instance = MapTableRow(
    intColumn = 1
    , longColumn = 1
    , mapStringColumn = Map("k1" -> "v1", "k2" -> "v2")
    , mapIntColumn = Map(1 -> "v1", 2 -> "v2")
  )
}