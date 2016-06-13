package spinoco.fs2.cassandra



trait DeleteSpec  extends SchemaSupport {


  s"DELETE statement (${cassandra.tag})" - {

    val delete =
      simpleTable.delete
        .row
        .build
        .fromA

    "will delete whole row" in withSessionAndSimpleSchema { cs =>

      cs.execute(delete)(1).unsafeRun

      val result = cs.queryAll(strSelectAll).runLog.unsafeRun

      result.forall(str => str.intColumn != 1) shouldBe true

    }

    "will delete single cluster from the row" in withSessionAndSimpleSchema { cs =>
      val delete =
        simpleTable.delete
          .row
          .cluster('longColumn)
          .build.fromTuple

      cs.execute(delete)(1 -> 1l).unsafeRun

      val result = cs.queryAll(strSelectAll).runLog.unsafeRun

      result.forall(str => ! (str.intColumn == 1 && str.longColumn == 1)) shouldBe true

    }

    "will delete single column from the row cluster" in withSessionAndOptionalSchema { cs =>
      val delete =
        optionalTable.delete
        .column('stringColumn)
        .primary
        .build.fromTuple

      cs.execute(delete)(1 -> 1l).unsafeRun

      val result = cs.queryAll(otSelectAll).runLog.unsafeRun

      result.forall(opr => ! (opr.intColumn == 1 &&  opr.longColumn == 1l &&  opr.stringColumn.nonEmpty)) shouldBe true

    }

    // below will fail seems Java driver (3.0.1) won't return result for IF/IF EXISTS
    // see https://datastax-oss.atlassian.net/browse/JAVA-1218
//    "will delete if exists" in withSessionAndSimpleSchema { cs =>
//      val delete =
//        simpleTable.delete
//          .row
//          .primary
//          .onlyIfExists
//          .build
//          .fromTuple
//          .asA
//
//
//
//      val result1 = cs.execute(delete)(1 -> 1l).unsafeRun
//      val result2 = cs.execute(delete)(99 -> 1l).unsafeRun
//
//      result1 shouldBe true
//      result2 shouldBe false
//
//    }
//
//    "will delete if condition holds" in withSessionAndSimpleSchema { cs =>
//      val delete =
//        simpleTable.delete
//          .row
//          .primary
//          .onlyIf('stringColumn, "str_eq", Comparison.EQ)
//          .build
//          .fromTriple
//          .asA
//
//
//      val result1 = cs.execute(delete)(("varchar string",1,1l)).unsafeRun
//      val result2 = cs.execute(delete)(("xxx",2,2l)).unsafeRun
//
//    }


  }

}
