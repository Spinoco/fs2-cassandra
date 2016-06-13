package spinoco.fs2.cassandra.sample

import shapeless.tag
import shapeless.tag._
import spinoco.fs2.cassandra.CType.Ascii

case class OptionalTableRow(
   intColumn: Int
   , longColumn: Long
   , stringColumn: Option[String]
   , asciiColumn: Option[String @@ Ascii]
   , enumColumn: Option[TestEnumeration.Value]
   , listColumn: Option[List[String]]
   , setColumn: Option[Set[String]]
   , vectorColumn: Option[Vector[String]]
 )


object OptionalTableRow {
  val instance = new OptionalTableRow(
   intColumn = 1
   , longColumn = 1l
   , stringColumn = Some("sc")
   , asciiColumn = Some(tag[Ascii]("a1"))
   , enumColumn= Some(TestEnumeration.Two)
   , listColumn = Some(List("one", "two"))
   , setColumn = Some(Set("o","t"))
   , vectorColumn = Some(Vector("v1", "v2"))
  )
}