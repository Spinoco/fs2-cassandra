package spinoco.fs2.cassandra
import fs2.Stream._
import fs2._
import spinoco.fs2.cassandra.sample.SimpleTableRow


class CrudSpec extends SchemaSupport {
  "Simple CRUD" - {

    "insert, update and delete SimpleTableRow" in withSession { cs =>

      val table =
        ks.table[SimpleTableRow]
          .partition('intColumn)
          .build("simple_table")

      val insert =
        table.insert.all.build.from[SimpleTableRow]

      val query =
        table.query.all.build.as[SimpleTableRow]

      val update =
        table.update.set('stringColumn).build.fromHList.fromTuple[(String,Int)]

      val delete =
        table.delete.row.build.fromA

      val entries = for (i <- 0 to 100) yield SimpleTableRow.simpleInstance.copy(intColumn = i)

      // create table
      (exec(cs.create(ks)) ++ exec(cs.create(table))).compile.drain.unsafeRunSync()

      // insert all
      emits(entries).flatMap { e => eval(cs.execute(insert)(e)).drain }.compile.drain.unsafeRunSync()

      // query all
      val result1 = cs.queryAll(query).compile.toVector.unsafeRunSync()

      // update some
      eval(cs.execute(update)("UPDATED" -> 99)).compile.drain.unsafeRunSync()

      // query with updated
      val result2 = cs.queryAll(query).compile.toVector.unsafeRunSync()

      // delete some
      eval(cs.execute(delete)(99)).compile.drain.unsafeRunSync()

      val result3 = cs.queryAll(query).compile.toVector.unsafeRunSync()

      result1.toSet shouldBe entries.toSet
      result2.find(_.intColumn == 99).map(_.stringColumn) shouldBe Some("UPDATED")
      result3.find(_.intColumn == 99) shouldBe None

    }


  }
}
