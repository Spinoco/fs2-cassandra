package spinoco.fs2.cassandra

import cats.effect.IO
import cats.syntax.all._
import com.datastax.oss.driver.api.core.DefaultProtocolVersion
import com.datastax.oss.driver.api.core.session.Request
import shapeless.{::, HNil}
import spinoco.fs2.cassandra.sample._

/**
  * The driver retries only idempotent statements on another node (e.g. when the connection drops before
  * the response arrives). Statements that change the result when repeated must not be marked idempotent,
  * even when the driver is configured with `basic.request.default-idempotence = true`.
  */
class IdempotenceSpec extends SchemaSupport {

  val counterTable =
    ks.table[CounterTableRow]
      .partition(Symbol("intColumn"))
      .cluster(Symbol("longColumn"))
      .build("counter_table")

  val insertIfNotExists =
    simpleTable.insert.all.ifNotExists.build.from[SimpleTableRow]

  /** None when not set on the statement, so the driver's `basic.request.default-idempotence` applies */
  def idempotence(r: Request): Option[Boolean] =
    Option(r.isIdempotent).map(_.booleanValue)

  def bound[I](cs: CassandraSession[IO], o: DMLOptions = Options.defaultDML)(statement: DMLStatement[I, _])(i: I): Option[Boolean] =
    idempotence(cs.bindStatement(statement, o)(i).unsafeRunSync())

  def withTables(f: CassandraSession[IO] => Any): Unit =
    withSession { cs =>
      (cs.create(ks) >> cs.create(simpleTable) >> cs.create(listTable) >> cs.create(counterTable)).unsafeRunSync()
      f(cs)
    }

  "Idempotence" - {

    "INSERT is left to the driver default" in withTables { cs =>
      bound(cs)(strInsert)(SimpleTableRow.simpleInstance) shouldBe None
    }

    "INSERT IF NOT EXISTS is not idempotent" in withTables { cs =>
      bound(cs)(insertIfNotExists)(SimpleTableRow.simpleInstance) shouldBe Some(false)
    }

    "UPDATE of regular columns is left to the driver default" in withTables { cs =>
      val update = simpleTable.update.set(Symbol("stringColumn")).build.fromHList.fromTuple[(String, Int, Long)]
      bound(cs)(update)(("x", 1, 1L)) shouldBe None
    }

    "UPDATE IF EXISTS is not idempotent" in withTables { cs =>
      val update = simpleTable.update.set(Symbol("stringColumn")).onlyIfExists.build.fromHList.fromTuple[(String, Int, Long)]
      bound(cs)(update)(("x", 1, 1L)) shouldBe Some(false)
    }

    "UPDATE IF condition is not idempotent" in withTables { cs =>
      val update =
        simpleTable.update
          .set(Symbol("stringColumn"))
          .onlyIf(Symbol("doubleColumn"), "double_gt", Comparison.GT)
          .build
          .fromHList
          .fromTuple[(Double, String, Int, Long)]

      bound(cs)(update)((0.0d, "x", 1, 1L)) shouldBe Some(false)
    }

    "counter UPDATE is not idempotent" in withTables { cs =>
      val increment = counterTable.update.increment(Symbol("counterColumn")).build.fromHList.fromTuple[(Long, Int, Long)]
      val decrement = counterTable.update.decrement(Symbol("counterColumn")).build.fromHList.fromTuple[(Long, Int, Long)]

      bound(cs)(increment)((1L, 1, 1L)) shouldBe Some(false)
      bound(cs)(decrement)((1L, 1, 1L)) shouldBe Some(false)
    }

    "list append and prepend are not idempotent" in withTables { cs =>
      val append = listTable.update.append(Symbol("listColumn")).build.fromHList.fromTuple[(List[String], Int, Long)]
      val prepend = listTable.update.prepend(Symbol("listColumn")).build.fromHList.fromTuple[(List[String], Int, Long)]
      // flag must survive further builder calls
      val appendAndSet =
        listTable.update
          .append(Symbol("listColumn"))
          .set(Symbol("setColumn"))
          .build
          .fromHList
          .fromTuple[(Set[String], List[String], Int, Long)]

      bound(cs)(append)((List("a"), 1, 1L)) shouldBe Some(false)
      bound(cs)(prepend)((List("a"), 1, 1L)) shouldBe Some(false)
      bound(cs)(appendAndSet)((Set("s"), List("a"), 1, 1L)) shouldBe Some(false)
    }

    "adding to a set is left to the driver default" in withTables { cs =>
      val add = listTable.update.add(Symbol("setColumn")).build.fromHList.fromTuple[(Set[String], Int, Long)]
      bound(cs)(add)((Set("a"), 1, 1L)) shouldBe None
    }

    "DELETE is left to the driver default" in withTables { cs =>
      val delete = simpleTable.delete.row.primary.build.fromHList.fromTuple[(Int, Long)]
      bound(cs)(delete)((1, 1L)) shouldBe None
    }

    "DELETE IF EXISTS and DELETE IF condition are not idempotent" in withTables { cs =>
      val deleteIfExists = simpleTable.delete.row.primary.onlyIfExists.build.fromHList.fromTuple[(Int, Long)]
      val deleteIf =
        simpleTable.delete.row.primary
          .onlyIf(Symbol("stringColumn"), "str_eq", Comparison.EQ)
          .build
          .fromHList
          .fromTuple[(String, Int, Long)]

      bound(cs)(deleteIfExists)((1, 1L)) shouldBe Some(false)
      bound(cs)(deleteIf)(("x", 1, 1L)) shouldBe Some(false)
    }

    "DMLOptions.setIdempotent overrides the builder" in withTables { cs =>
      val increment = counterTable.update.increment(Symbol("counterColumn")).build.fromHList.fromTuple[(Long, Int, Long)]
      bound(cs, Options.defaultDML.setIdempotent(true))(increment)((1L, 1, 1L)) shouldBe Some(true)
    }

    "queries are idempotent" in withTables { cs =>
      val ps = cs.prepareCql(strSelectOne.cqlStatement).unsafeRunSync()
      idempotence(strSelectOne.fill((1, 1L), ps, DefaultProtocolVersion.V4)) shouldBe Some(true)
    }

    "batch is not idempotent when any of its statements is not idempotent" in withTables { cs =>
      val plain = batch.logged.add(strInsert).add(strInsert).build
      val mixed = batch.logged.add(strInsert).add(insertIfNotExists).build
      val rows = SimpleTableRow.simpleInstance :: SimpleTableRow.simpleInstance.copy(longColumn = 2) :: HNil

      def create[R](b: BatchStatement[SimpleTableRow :: SimpleTableRow :: HNil, R]): Option[Boolean] = {
        val prepared = b.statements.toList.traverse(cs.prepareCql).unsafeRunSync()
        idempotence(b.createStatement(prepared, rows, DefaultProtocolVersion.V4).fold(throw _, identity))
      }

      create(plain) shouldBe None
      create(mixed) shouldBe Some(false)
    }

    "raw batch is not idempotent when any of its statements is not idempotent" in withTables { cs =>
      // conditional batch must stay in single partition
      val row1 = SimpleTableRow.simpleInstance
      val row2 = SimpleTableRow.simpleInstance.copy(longColumn = 2)

      val plain = List(cs.bindStatement(strInsert)(row1), cs.bindStatement(strInsert)(row2)).sequence.unsafeRunSync()
      val mixed = List(cs.bindStatement(strInsert)(row1), cs.bindStatement(insertIfNotExists)(row2)).sequence.unsafeRunSync()

      // checks the statement the driver actually executed
      idempotence(cs.executeBatchRaw(plain, logged = true).unsafeRunSync().getExecutionInfo.getRequest) shouldBe None
      idempotence(cs.executeBatchRaw(mixed, logged = true).unsafeRunSync().getExecutionInfo.getRequest) shouldBe Some(false)
    }

  }

}
