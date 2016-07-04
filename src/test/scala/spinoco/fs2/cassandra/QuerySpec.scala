package spinoco.fs2.cassandra

import shapeless.tag
import shapeless.tag._
import spinoco.fs2.cassandra.CType.Ascii
import spinoco.fs2.cassandra.sample.SimpleTableRow


trait QuerySpec extends SchemaSupport {

  "Specific queries" - {

    "aliased cluster column statement" in  withSessionAndSimpleSchema { cs =>
      val query =
        simpleTable.query.all
          .partition.cluster('longColumn, 'lv, Comparison.GTEQ)
          .build
          .fromHList
          .fromTuple[(Int, Long)]
          .as[SimpleTableRow]

      val r = cs.query(query)(1 -> 9l).runLog.unsafeRun

      r.size shouldBe 2
      r.map(_.longColumn) shouldBe Vector(9, 10)
    }


    "will query by index" in  withSessionAndSimpleSchema { cs =>
      val update =
        simpleTable.update.set('asciiColumn)
          .build
          .fromHList.fromTuple[(String @@ Ascii, Int, Long)]

      cs.execute(update)((tag[Ascii]("ascii1"), 1, 1)).unsafeRun()

      val query =
        simpleTable.query.all
          .byIndex('asciiColumn, Comparison.EQ)
          .build
          .fromA[String @@ Ascii]
          .as[SimpleTableRow]

      val r = cs.query(query)(tag[Ascii]("ascii1")).runLog.unsafeRun

      r.size shouldBe 1
      r.map(_.asciiColumn) shouldBe Vector(tag[Ascii]("ascii1"))

    }

  }

}
