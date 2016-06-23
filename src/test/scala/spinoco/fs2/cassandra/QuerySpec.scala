package spinoco.fs2.cassandra

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

  }

}
