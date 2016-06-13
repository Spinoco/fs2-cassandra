package spinoco.fs2.cassandra


import shapeless. tag
import spinoco.fs2.cassandra.sample.SimpleTableRow
import shapeless.syntax.singleton._
import spinoco.fs2.cassandra.CType.TTL

import scala.concurrent.duration._

/**
  * Created by pach on 11/06/16.
  */
trait InsertSpec extends SchemaSupport {



  s"INSERT statement (${cassandra.tag})" - {

    "will insert IF NOT EXISTS " in withSessionAndEmptySimpleSchema { cs =>

      val insert =
        simpleTable.insert
          .all
          .ifNotExists
          .build
          .from[SimpleTableRow]
          .as[SimpleTableRow]

      val result1 = cs.execute(insert)(SimpleTableRow.simpleInstance).unsafeRun

      val result2 = cs.execute(insert)(SimpleTableRow.simpleInstance).unsafeRun

      result1 shouldBe None
      result2 shouldBe Some(SimpleTableRow.simpleInstance)

    }

    "will insert with TTL" in withSessionAndEmptySimpleSchema { cs =>
      val insert =
        simpleTable.insert
          .all
          .withTTL('ttl)
          .build
          .mapIn[(FiniteDuration, SimpleTableRow)] { case (dur, row) => ('ttl ->> tag[TTL](dur)) +: strGen.to(row) }

      //insert filed with ttl
      cs.execute(insert)(1.hour -> SimpleTableRow.simpleInstance).unsafeRun

      //insert filed without ttl
      cs.execute(strInsert)(SimpleTableRow.simpleInstance.copy(intColumn = 2)).unsafeRun


      val selectTTL =
        simpleTable.query
          .functionAt(functions.ttlOf[String], 'stringColumn, 'ttl)
          .partition
          .build
          .fromA
          .asA

      val resultTTL = cs.query(selectTTL)(1).runLog.unsafeRun
      val resultNoTTL = cs.query(selectTTL)(2).runLog.unsafeRun

      resultTTL.map(_.nonEmpty) shouldBe Vector(true)
      resultNoTTL.map(_.nonEmpty) shouldBe Vector(false)

    }


    "will insert with timestamp" in withSessionAndEmptySimpleSchema { cs =>

      val insert =
        simpleTable.insert
          .all
          .withTimestamp('ts)
          .build
          .mapIn[(Long, SimpleTableRow)] { case (ts, row) => ('ts ->> ts) +: strGen.to(row) }


      //insert filed with ts
      cs.execute(insert)(999l-> SimpleTableRow.simpleInstance).unsafeRun


      val select =
        simpleTable.query
          .functionAt(functions.writeTimeOfMicro[String], 'stringColumn, 'ts)
          .partition
          .build
          .fromA
          .asA

      val result = cs.query(select)(1).runLog.unsafeRun

     result shouldBe Vector(999l)

    }


    "will insert with ttl, timestamp" in withSessionAndEmptySimpleSchema { cs =>

      val insert =
        simpleTable.insert
          .all
          .withTimestamp('ts)
          .withTTL('ttl)
          .build
          .as[SimpleTableRow]
          .mapIn[(FiniteDuration, Long, SimpleTableRow)] { case (dur, ts, row) => ('ttl ->> tag[TTL](dur)) +: (('ts ->> ts) +: strGen.to(row)) }


      cs.execute(insert)((1.hour,999l, SimpleTableRow.simpleInstance)).unsafeRun

      val select =
        simpleTable.query
          .functionAt(functions.writeTimeOfMicro[String], 'stringColumn, 'ts)
          .functionAt(functions.ttlOf[String], 'stringColumn, 'ttl)
          .partition
          .build
          .fromA
          .asTuple


      val resultQ = cs.query(select)(1).runLog.unsafeRun

      resultQ.map{ case (ttl, ts) => ttl.nonEmpty -> ts} shouldBe Vector(true -> 999l)

    }


  }

}
