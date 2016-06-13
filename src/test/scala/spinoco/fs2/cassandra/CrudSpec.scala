package spinoco.fs2.cassandra

import fs2.Stream._
import spinoco.fs2.cassandra.sample.SimpleTableRow


trait CrudSpec extends SchemaSupport {

  s"Simple CRUD (${cassandra.tag})" - {

    "insert, update and delete SimpleTableRow" in withCluster { c =>

      val table =
        ks.table[SimpleTableRow]
          .partition('intColumn)
          .createTable("simple_table")

      val insert =
        table.insert.all.build.from[SimpleTableRow]

      val query =
        table.query.all.build.as[SimpleTableRow]

      val update =
        table.update.set('stringColumn).build.fromTuple

      val delete =
        table.delete.row.build.fromA

      val entries = for (i <- 0 to 100) yield SimpleTableRow.simpleInstance.copy(intColumn = i)

      // create table
      c.session .flatMap { cs => eval_(cs.create(ks)) ++ eval_(cs.create(table)) }.run.unsafeRun

      // insert all
      c.session .flatMap { cs => emits(entries).flatMap { e => eval_(cs.execute(insert)(e)) }}.run.unsafeRun

      // query all
      val result1 = c.session.flatMap { _.queryAll(query) }.runLog.unsafeRun

      // update some
      c.session.flatMap { cs => eval_(cs.execute(update)("UPDATED" -> 99)) }.run.unsafeRun

      // query with updated
      val result2 = c.session.flatMap { _.queryAll(query) }.runLog.unsafeRun

      // delete some
      c.session.flatMap { cs => eval_(cs.execute(delete)(99)) }.run.unsafeRun

      val result3 = c.session.flatMap { _.queryAll(query) }.runLog.unsafeRun

      result1.toSet shouldBe entries.toSet
      result2.find(_.intColumn == 99).map(_.stringColumn) shouldBe Some("UPDATED")
      result3.find(_.intColumn == 99) shouldBe None

    }


  }

}
