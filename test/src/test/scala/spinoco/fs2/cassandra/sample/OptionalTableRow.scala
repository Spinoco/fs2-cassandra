package spinoco.fs2.cassandra.sample

import shapeless.tag
import shapeless.tag._
import spinoco.fs2.cassandra.CType.Ascii

case class OptionalTableRow(
   intColumn: Int
   , longColumn: Long
   , boolColumn: Option[Boolean]
   , maybeIntColumn: Option[Int]
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
   , boolColumn = Some(true)
   , maybeIntColumn = Some(1)
   , stringColumn = Some("sc")
   , asciiColumn = Some(tag[Ascii]("a1"))
   , enumColumn= Some(TestEnumeration.Two)
   , listColumn = Some(List("one", "two"))
   , setColumn = Some(Set("o","t"))
   , vectorColumn = Some(Vector("v1", "v2"))
  )

 val emptyInstance = new OptionalTableRow(
   intColumn =11
   , longColumn = 11l
   , boolColumn = Some(false)
   , maybeIntColumn = Some(0)
   , stringColumn = Some("")
   , asciiColumn = Some(tag[Ascii](""))
   , enumColumn= None
   // Collections are coerced to null by cassandra
   , listColumn = None
   , setColumn = None
   , vectorColumn = None
  )

  val noneInstance = new OptionalTableRow(
    intColumn =12
    , longColumn = 12l
    , boolColumn = None
    , maybeIntColumn = None
    , stringColumn = None
    , asciiColumn = None
    , enumColumn= None
    , listColumn = None
    , setColumn = None
    , vectorColumn = None
  )
}