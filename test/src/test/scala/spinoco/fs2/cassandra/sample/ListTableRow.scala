package spinoco.fs2.cassandra.sample

case class ListTableRow(
   intColumn: Int
   , longColumn: Long
   , listColumn: List[String]
   , setColumn: Set[String]
   , vectorColumn: Vector[String]
   , seqColumn: Seq[String]
 )


object ListTableRow {
  val instance = ListTableRow(
    intColumn = 1
    , longColumn = 1
    , listColumn = List("one","two")
    , setColumn = Set("ones", "twos")
    , vectorColumn = Vector("onev", "twov")
    , seqColumn = Seq("oneq", "twoq")
  )
}
