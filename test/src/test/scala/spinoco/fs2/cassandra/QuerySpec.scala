package spinoco.fs2.cassandra
import shapeless.tag
import shapeless.tag._
import spinoco.fs2.cassandra.ctype.CType.Ascii
import spinoco.fs2.cassandra.sample.SimpleTableRow


class QuerySpec extends SchemaSupport {

  "Specific queries" - {

    "aliased cluster column statement" in  withSessionAndSimpleSchema { cs =>
      val query =
        simpleTable.query.all
          .partition.cluster(Symbol("longColumn"), Symbol("lv"), Comparison.GTEQ)
          .build
          .fromHList
          .fromTuple[(Int, Long)]
          .as[SimpleTableRow]

      val r = cs.query(query)(1 -> 9L).compile.toVector.unsafeRunSync()

      r.size shouldBe 2
      r.map(_.longColumn) shouldBe Vector(9, 10)
    }


    "will query by index" in  withSessionAndSimpleSchema { cs =>
      val update =
        simpleTable.update.set(Symbol("asciiColumn"))
          .build
          .fromHList.fromTuple[(String @@ Ascii, Int, Long)]

      cs.execute(update)((tag[Ascii]("ascii1"), 1, 1)).unsafeRunSync()

      val query =
        simpleTable.query.all
          .byIndex(Symbol("asciiColumn"), Comparison.EQ)
          .build
          .fromA[String @@ Ascii]
          .as[SimpleTableRow]

      val r = cs.query(query)(tag[Ascii]("ascii1")).compile.toVector.unsafeRunSync()

      r.size shouldBe 1
      r.map(_.asciiColumn) shouldBe Vector(tag[Ascii]("ascii1"))

    }

  }

}
