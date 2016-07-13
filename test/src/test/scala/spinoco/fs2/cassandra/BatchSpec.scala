package spinoco.fs2.cassandra

import shapeless.HNil
import spinoco.fs2.cassandra.sample.SimpleTableRow


trait BatchSpec extends SchemaSupport {

  s"BATCH statement (${cassandra.tag})" - {

    "will construct simple batch logged statement" in withSessionAndEmptySimpleSchema { cs =>

      val bs =
        batch.logged
          .add(strInsert)
          .add(strInsert)
          .build

      cs.executeBatch(bs)(
        SimpleTableRow.simpleInstance ::
          SimpleTableRow.simpleInstance.copy(intColumn = 2) ::
          HNil
      ).unsafeRun

      val result = cs.queryAll(strSelectAll).runLog.unsafeRun

      result.toSet shouldBe Set(
        SimpleTableRow.simpleInstance
        , SimpleTableRow.simpleInstance.copy(intColumn = 2)
      )

    }


    "will construct simple unlogged batch statement" in withSessionAndEmptySimpleSchema { cs =>

      val bs =
        batch.unLogged
          .add(strInsert)
          .add(strInsert)
          .build

      cs.executeBatch(bs)(
        SimpleTableRow.simpleInstance ::
          SimpleTableRow.simpleInstance.copy(intColumn = 2) ::
          HNil
      ).unsafeRun

      val result = cs.queryAll(strSelectAll).runLog.unsafeRun

      result.toSet shouldBe Set(
        SimpleTableRow.simpleInstance
        , SimpleTableRow.simpleInstance.copy(intColumn = 2)
      )

    }


    "will return current value for ifExists condition" in withSessionAndEmptySimpleSchema { cs =>

      val insertIfNotExists =
      simpleTable.insert
        .all.ifNotExists
        .build
        .from[SimpleTableRow]
        .as[SimpleTableRow]

      cs.execute(strInsert)(SimpleTableRow.simpleInstance).unsafeRun
      cs.execute(strInsert)(SimpleTableRow.simpleInstance.copy(longColumn = 20)).unsafeRun

      val bs =
        batch.unLogged
          .add(insertIfNotExists)
          .add(insertIfNotExists)
          .build

      val result =
      cs.executeBatch(bs)(
        SimpleTableRow.simpleInstance ::
          SimpleTableRow.simpleInstance.copy(longColumn = 30) ::
          HNil
      ).unsafeRun

      result shouldBe Some(Some(Some(SimpleTableRow.simpleInstance)) :: None :: HNil)

    }


    "will return both values, if both fails" in withSessionAndEmptySimpleSchema { cs =>
      val insertIfNotExists =
        simpleTable.insert
          .all.ifNotExists
          .build
          .from[SimpleTableRow]
          .as[SimpleTableRow]

      cs.execute(strInsert)(SimpleTableRow.simpleInstance).unsafeRun
      cs.execute(strInsert)(SimpleTableRow.simpleInstance.copy(longColumn = 20)).unsafeRun

      val bs =
        batch.unLogged
          .add(insertIfNotExists)
          .add(insertIfNotExists)
          .build

      val result =
        cs.executeBatch(bs)(
          SimpleTableRow.simpleInstance ::
            SimpleTableRow.simpleInstance.copy(longColumn = 20) ::
            HNil
        ).unsafeRun


      result shouldBe Some(Some(Some(SimpleTableRow.simpleInstance)) :: Some(Some(SimpleTableRow.simpleInstance.copy(longColumn = 20))) :: HNil)
    }


// all  tests below are subject to https://datastax-oss.atlassian.net/browse/JAVA-1221
//    "will perform ifExists update" in withSessionAndEmptySimpleSchema { cs =>
//
//      cs.execute(strInsert)(SimpleTableRow.simpleInstance).unsafeRun
//      cs.execute(strInsert)(SimpleTableRow.simpleInstance.copy(longColumn = 20)).unsafeRun
//
//      cs.queryAll(strSelectAll).runLog.unsafeRun.foreach(println)
//
//      val updateIfExists =
//        simpleTable.update
//        .set('stringColumn)
//        .onlyIfExists
//        .build
//        .fromTriple
//        .asA
//
//      val bs =
//        batch.logged
//          .add(updateIfExists)
//          .add(updateIfExists)
//          .build
//
//      val result =
//        cs.executeBatch(bs)(
//          ("U1",1,2l) ::
//            ("U2",1,20l)::
//            HNil
//        ).unsafeRun
//
//
//      result shouldBe None
//
//      cs.queryAll(strSelectAll).runLog.unsafeRun.foreach(println)
//
//      val cs1 = sessionInstance.map(_._1).get
//      val r1 = cs1.execute(s"SELECT * from ${simpleTable.fullName}")
//      r1.all().asScala.foreach(println)
//    }
//
//
//    "will return err on ifExists update" in withSessionAndEmptySimpleSchema { cs =>
//
//      cs.execute(strInsert)(SimpleTableRow.simpleInstance).unsafeRun
//      cs.execute(strInsert)(SimpleTableRow.simpleInstance.copy(longColumn = 20)).unsafeRun
//
//      val updateIfExists =
//        simpleTable.update
//          .set('stringColumn)
//          .onlyIfExists
//          .build
//          .fromTriple
//          .asA
//
//      val bs =
//        batch.logged
//          .add(updateIfExists)
//          .add(updateIfExists)
//          .build
//
//      val result =
//        cs.executeBatch(bs)(
//          ("U1",1,3l) ::
//            ("U2",1,30l)::
//            HNil
//        ).unsafeRun
//
//
//      result shouldBe (None :: None :: HNil)
//
//      cs.queryAll(strSelectAll).runLog.unsafeRun.foreach(println)
//
//    }
//
//      "will return err on if update" in withSessionAndEmptySimpleSchema { cs =>
//
//        cs.execute(strInsert)(SimpleTableRow.simpleInstance).unsafeRun
//        cs.execute(strInsert)(SimpleTableRow.simpleInstance.copy(longColumn = 20)).unsafeRun
//
//        val updateIfEq =
//          simpleTable.update
//            .set('stringColumn)
//            .onlyIf('stringColumn, "str_eq", Comparison.EQ)
//            .build
//            .fromTuple4
//            .asA
//
//        val bs =
//          batch.logged
//            .add(updateIfEq)
//            .add(updateIfEq)
//            .build
//
//        val result =
//          cs.executeBatch(bs)(
//            ("varchar string", "U1",1,2l) ::
//              ("varchar string", "U2",1,20l) ::
//              HNil
//          ).unsafeRun
//
//
//        result shouldBe (None :: None :: HNil)
//
//        cs.queryAll(strSelectAll).runLog.unsafeRun.foreach(println)
//
//      }



  }

}
