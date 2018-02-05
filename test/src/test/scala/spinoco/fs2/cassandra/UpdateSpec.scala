package spinoco.fs2.cassandra


import java.net.InetAddress

import fs2.Chunk
import fs2.Stream._
import shapeless.tag
import spinoco.fs2.cassandra.CType.{Ascii, Counter, TTL, Type1}
import spinoco.fs2.cassandra.sample._

import scala.concurrent.duration._
import com.datastax.driver.core.utils.UUIDs.timeBased
import shapeless.tag.{apply => _, _}



trait UpdateSpec extends SchemaSupport {



  s"UPDATE statement (${cassandra.tag})" - {

    "will update all given columns" in withSessionAndSimpleSchema { cs =>
      val update =
        simpleTable
          .update.all
          .build
          .from[SimpleTableRow]

      val modified =
      SimpleTableRow.simpleInstance.copy(
        intColumn = 9
        , longColumn = 9
        , stringColumn = "updated"
        , asciiColumn = tag[Ascii]("ascii updated")
        , floatColumn = 0f
        , doubleColumn = 0d
        , bigDecimalColumn = BigDecimal(0)
        , bigIntColumn = BigInt(0)
        , blobColumn = Chunk.bytes(Array(1,2,3))
        , uuidColumn =  timeBased
        , timeUuidColumn =  tag[Type1](timeBased)
        , durationColumn = FiniteDuration(1,"min")
        , inetAddressColumn = InetAddress.getByName("www.google.com")
        , enumColumn = TestEnumeration.Two
      )

      cs.execute(update)(modified).unsafeRunSync()



      val result = cs.query(strSelectOne)(9 -> 9l).compile.toVector.unsafeRunSync()


      result shouldBe Vector(modified)
    }


    "will update single column"  in withSessionAndSimpleSchema { cs =>
      val update =
        simpleTable
          .update
          .set('stringColumn)
          .build
          .fromHList
          .fromTuple[(String,Int,Long)]

      cs.execute(update)(("UPDATED",9,9)).unsafeRunSync()


      val result = cs.query(strSelectOne)(9 -> 9l).compile.toVector.unsafeRunSync()

      result.map(_.stringColumn) shouldBe Vector("UPDATED")
    }

    "will update single row if exists"   in withSessionAndSimpleSchema { cs =>
      val update =
        simpleTable
          .update
          .set('stringColumn)
          .onlyIfExists
          .build
          .fromHList
          .fromTuple[(String,Int,Long)]
          .asA

      val result1 =
        cs.execute(update)(("UPDATED", 9, 9)).unsafeRunSync()

      val result2 = cs.execute(update)(("UPDATED", 11, 1)).unsafeRunSync()


      result1 shouldBe true
      result2 shouldBe false
    }

    "will update single row if condition" in withSessionAndSimpleSchema { cs =>
      val update =
        simpleTable
          .update
          .set('stringColumn)
          .onlyIf('doubleColumn, "double_gt", Comparison.GT)
          .build
          .fromHList
          .fromTuple[(Double,String,Int,Long)]
          .asA


      val result1 = cs.execute(update)((0.0d,"UPDATED", 9, 9)).unsafeRunSync()

      val result2 =cs.execute(update)((999d, "UPDATED", 9, 9)).unsafeRunSync()




      result1 shouldBe None
      result2 shouldBe Some(2.2d)

    }

    "will update single row if conditions" in withSessionAndSimpleSchema { cs =>
      val update =
        simpleTable
          .update
          .set('stringColumn)
          .onlyIf('doubleColumn, Comparison.GT)
          .onlyIf('floatColumn, "float_greater",Comparison.GT)
          .build
          .fromHList
          .fromTuple[(Float,Double,String,Int,Long)]
          .asTuple

      val result1 = cs.execute(update)((0.0f, 0.0d, "UPDATED", 9, 9)).unsafeRunSync()

      val result2 = cs.execute(update)((999f, 999d, "UPDATED", 9, 9)).unsafeRunSync()


      result1 shouldBe (None -> None)
      result2 shouldBe (Some(1.1f) -> Some(2.2d)) // updated by result1, but less than 999d
    }


    "will update list column" in withSessionAndListSchema { cs =>
      val update =
        listTable
          .update
          .set('listColumn)
          .build
          .fromHList
          .fromTuple[(List[String],Int,Long)]

        cs.execute(update)((List("l1","l2"), 9, 9)).unsafeRunSync()



      val result = cs.query(ltSelectOne)(9 -> 9).compile.toVector.unsafeRunSync()

      result shouldBe Vector(
        ListTableRow.instance.copy(
          intColumn = 9
          , longColumn = 9
          , listColumn = List("l1","l2")
        )
      )

    }


    "will append to list column"  in withSessionAndListSchema { cs =>
      val update =
      listTable
        .update
        .append('listColumn)
        .build
        .fromHList
        .fromTuple[(List[String],Int,Long)]


      cs.execute(update)((List("appended"), 9, 9)).unsafeRunSync()

      val result = cs.query(ltSelectOne)(9 -> 9).compile.toVector.unsafeRunSync()

      result shouldBe Vector(
        ListTableRow.instance.copy(
          intColumn = 9
          , longColumn = 9
          , listColumn = List("one","two","appended")
        )
      )

    }


    "will prepend to list column"  in withSessionAndListSchema { cs =>
      val update =
        listTable
          .update
          .prepend('listColumn)
          .build
          .fromHList
          .fromTuple[(List[String],Int,Long)]

      cs.execute(update)((List("prepended"), 9, 9)).unsafeRunSync()

      val result = cs.query(ltSelectOne)(9 -> 9).compile.toVector.unsafeRunSync()

      result shouldBe Vector(
        ListTableRow.instance.copy(
          intColumn = 9
          , longColumn = 9
          , listColumn = List("prepended","one","two")
        )
      )

    }

    "will set item at index in column"  in withSessionAndListSchema { cs =>
      val update =
        listTable
          .update
          .addAt('listColumn, 0)
          .build
          .fromHList
          .fromTuple[(String,Int,Long)]

      cs.execute(update)(("index_set", 9, 9)).unsafeRunSync()

      val result = cs.query(ltSelectOne)(9 -> 9).compile.toVector.unsafeRunSync()

      result shouldBe Vector(
        ListTableRow.instance.copy(
          intColumn = 9
          , longColumn = 9
          , listColumn = List("index_set","two")
        )
      )

    }

    "will remove from list column" in withSessionAndListSchema { cs =>
      val update =
        listTable
          .update
          .remove('listColumn)
          .build
          .fromHList
          .fromTuple[(List[String],Int,Long)]

      cs.execute(update)((List("two"), 9, 9)).unsafeRunSync()

      val result = cs.query(ltSelectOne)(9 -> 9).compile.toVector.unsafeRunSync()

      result shouldBe Vector(
        ListTableRow.instance.copy(
          intColumn = 9
          , longColumn = 9
          , listColumn = List("one")
        )
      )

    }

    "will add entry to set" in  withSessionAndListSchema { cs =>
      val update =
        listTable
          .update
          .add('setColumn)
          .build
          .fromHList
          .fromTuple[(Set[String],Int,Long)]

      cs.execute(update)((Set("added"), 9, 9)).unsafeRunSync()

      val result = cs.query(ltSelectOne)(9 -> 9).compile.toVector.unsafeRunSync()

      result shouldBe Vector(
        ListTableRow.instance.copy(
          intColumn = 9
          , longColumn = 9
          , setColumn = Set("ones", "twos", "added")
        )
      )

    }

    "will remove entry from set" in  withSessionAndListSchema { cs =>
      val update =
        listTable
          .update
          .remove('setColumn)
          .build
          .fromHList
          .fromTuple[(Set[String],Int,Long)]

      cs.execute(update)((Set("ones"), 9, 9)).unsafeRunSync()

      val result = cs.query(ltSelectOne)(9 -> 9).compile.toVector.unsafeRunSync()

      result shouldBe Vector(
        ListTableRow.instance.copy(
          intColumn = 9
          , longColumn = 9
          , setColumn = Set("twos")
        )
      )

    }


    "will add entry to map" in withSessionAndMapSchema { cs =>
      val update =
        mapTable
          .update
          .addToMap('mapStringColumn)
          .build
          .fromHList
          .fromTuple[(Map[String,String],Int,Long)]

      cs.execute(update)((Map("add" -> "addedValue"), 9,9)).unsafeRunSync()

      val result = cs.query(mtSelectOne)(9 -> 9).compile.toVector.unsafeRunSync()

      result shouldBe Vector(
        MapTableRow.instance.copy(
          intColumn = 9
          , longColumn = 9
          , mapStringColumn = MapTableRow.instance.mapStringColumn ++ Map("add" -> "addedValue")
        )
      )

    }


    "will remove entry from map" in withSessionAndMapSchema { cs =>
      val update =
        mapTable
          .update
          .removeFromMap('mapStringColumn)
          .build
          .fromHList
          .fromTuple[(Set[String],Int,Long)]

      cs.execute(update)((Set("k1"), 9,9)).unsafeRunSync()

      val result = cs.query(mtSelectOne)(9 -> 9).compile.toVector.unsafeRunSync()

      result shouldBe Vector(
        MapTableRow.instance.copy(
          intColumn = 9
          , longColumn = 9
          , mapStringColumn = MapTableRow.instance.mapStringColumn - "k1"
        )
      )

    }


    "will update with ttl" in withSessionAndSimpleSchema { cs =>

      val update =
        simpleTable
          .update
          .set('stringColumn)
          .withTTL('ttl)
          .build
          .fromHList
          .fromTuple[(FiniteDuration @@ TTL, String,Int,Long)]

      val selectTTL =
        simpleTable.query
        .functionAt(functions.ttlOf[String],'stringColumn, 'ttl)
        .primary
        .build
        .fromHList
        .fromTuple[(Int,Long)]
        .asA


      cs.execute(update)((tag[TTL](1.hour),"UPDATED",9,9)).unsafeRunSync()

      val result = cs.query(selectTTL)(9 -> 9l).compile.toVector.unsafeRunSync()

      result.map(_.isDefined) shouldBe Vector(true)

    }



    "will update with timestamp" in withSessionAndSimpleSchema { cs =>
      val update =
        simpleTable
          .update
          .set('stringColumn)
          .withTimeStamp('ts)
          .build
          .fromHList
          .fromTuple[(Long, String,Int,Long)]



      val selectTimeStamp =
        simpleTable.query
          .functionAt(functions.writeTimeOfMicro[String],'stringColumn, 'ts)
          .primary
          .build
          .fromHList
          .fromTuple[(Int,Long)]
          .asA

      val ts = System.currentTimeMillis()*1000 + 1

      cs.execute(update)((ts,"UPDATED",9,9)).unsafeRunSync()

      val result = cs.query(selectTimeStamp)(9 -> 9l).compile.toVector.unsafeRunSync()

      result shouldBe Vector(ts)

    }


    "will increment and decrement counter" in withSession { cs =>



      val counterTable =
        ks.table[CounterTableRow]
          .partition('intColumn)
          .cluster('longColumn)
          .build("counter_table")

      val increment =
        counterTable.update
          .increment('counterColumn)
          .build
          .fromHList
          .fromTuple[(Long, Int,Long)]

      val decrement =
        counterTable.update
          .decrement('counterColumn)
          .build
          .fromHList
          .fromTuple[(Long, Int,Long)]

      val select =
        counterTable.query.all.build.as[CounterTableRow]


      cs.create(ks).flatMap(_ => cs.create(counterTable)).unsafeRunSync()

      cs.execute(increment)((10, 1, 1)).unsafeRunSync()

      cs.execute(decrement)((5,1,1)).unsafeRunSync()

      cs.queryAll(select).compile.toVector.unsafeRunSync() shouldBe Vector(
        CounterTableRow(1,1l,tag[Counter](5l))  // +10 -5 = 5
      )

    }




  }
}
