package spinoco.fs2.cassandra



trait DeleteSpec  extends SchemaSupport {


  s"DELETE statement (${cassandra.tag})" - {

    val delete =
      simpleTable.delete
        .row
        .build
        .fromA

    "will delete whole row" in withSessionAndSimpleSchema { cs =>

      cs.execute(delete)(1).unsafeRunSync()

      val result = cs.queryAll(strSelectAll).compile.toVector.unsafeRunSync()

      result.forall(str => str.intColumn != 1) shouldBe true

    }

    "will delete single cluster from the row" in withSessionAndSimpleSchema { cs =>
      val delete =
        simpleTable.delete
          .row
          .cluster('longColumn)
          .build.fromHList.fromTuple[(Int,Long)]

      cs.execute(delete)(1 -> 1l).unsafeRunSync()

      val result = cs.queryAll(strSelectAll).compile.toVector.unsafeRunSync()

      result.forall(str => ! (str.intColumn == 1 && str.longColumn == 1)) shouldBe true

    }

    "will delete single column from the row cluster" in withSessionAndOptionalSchema { cs =>
      val delete =
        optionalTable.delete
        .column('stringColumn)
        .primary
        .build.fromHList.fromTuple[(Int,Long)]

      cs.execute(delete)(1 -> 1l).unsafeRunSync()

      val result = cs.queryAll(otSelectAll).compile.toVector.unsafeRunSync()

      result.forall(opr => ! (opr.intColumn == 1 &&  opr.longColumn == 1l &&  opr.stringColumn.nonEmpty)) shouldBe true

    }

    "will delete if exists" in withSessionAndSimpleSchema { cs =>
      val delete =
        simpleTable.delete
          .row
          .primary
          .onlyIfExists
          .build
          .fromHList
          .fromTuple[(Int,Long)]
          .asA


      val result1 =  cs.execute(delete)(1 -> 1l).unsafeRunSync()
      val result2 =  cs.execute(delete)(99 -> 1l).unsafeRunSync()

      result1 shouldBe true
      result2 shouldBe false

    }

    "will delete if condition holds" in withSessionAndSimpleSchema { cs =>
      val delete =
        simpleTable.delete
          .row
          .primary
          .onlyIf('stringColumn, "str_eq", Comparison.EQ)
          .build
          .fromHList
          .fromTuple[(String,Int,Long)]
          .asA


      val result1 = cs.execute(delete)(("varchar string",1,1l)).unsafeRunSync()
      val result2 = cs.execute(delete)(("xxx",2,2l)).unsafeRunSync()

      result1 shouldBe None
      result2 shouldBe Some("varchar string")

    }


  }

}
