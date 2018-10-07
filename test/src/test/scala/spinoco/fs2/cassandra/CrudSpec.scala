package spinoco.fs2.cassandra

import fs2._
import Stream._
import spinoco.fs2.cassandra.sample.SimpleTableRow


trait CrudSpec extends SchemaSupport {

  s"Simple CRUD (${cassandra.tag})" - {

    "insert, update and delete SimpleTableRow" in withCluster { c =>

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
      Stream.resource(c.session) .flatMap { cs => eval_(cs.create(ks)) ++ eval_(cs.create(table)) }.compile.drain.unsafeRunSync()

      // insert all
      Stream.resource(c.session) .flatMap { cs => emits(entries).flatMap { e => eval_(cs.execute(insert)(e)) }}.compile.drain.unsafeRunSync()

      // query all
      val result1 = Stream.resource(c.session).flatMap { _.queryAll(query) }.compile.toVector.unsafeRunSync()

      // update some
      Stream.resource(c.session).flatMap { cs => eval_(cs.execute(update)("UPDATED" -> 99)) }.compile.toVector.unsafeRunSync()

      // query with updated
      val result2 = Stream.resource(c.session).flatMap { _.queryAll(query) }.compile.toVector.unsafeRunSync()

      // delete some
      Stream.resource(c.session).flatMap { cs => eval_(cs.execute(delete)(99)) }.compile.drain.unsafeRunSync()

      val result3 = Stream.resource(c.session).flatMap { _.queryAll(query) }.compile.toVector.unsafeRunSync()

      result1.toSet shouldBe entries.toSet
      result2.find(_.intColumn == 99).map(_.stringColumn) shouldBe Some("UPDATED")
      result3.find(_.intColumn == 99) shouldBe None

    }


  }

}
