package spinoco.fs2.cassandra

import java.util.UUID

import shapeless.HNil
import spinoco.fs2.cassandra.sample.SimpleTableRow

/**
  * Created by adamchlupacek on 10/08/16.
  */
trait MaterializedSpec extends SchemaSupport{

  s" Materialized view (${cassandra.tag})" - {

    "Will create and query view" in withSessionAndEmptySimpleSchema { cs =>
      if(cassandra.isV3Compatible){
        val matView =
          simpleTable.query
          .column('floatColumn)
          .materialize
          .partition('uuidColumn)
          .cluster('intColumn)
          .cluster('longColumn)
          .build("mat")

        val matQuery =
          matView.query.all.partition.build.fromHList.mapIn[UUID]{case id =>
            id :: HNil
          }.asHlist

        val t =
          for {
            _ <- cs.create(matView)
            _ <- cs.execute(strInsert)(SimpleTableRow.simpleInstance)
          } yield ()

        t.unsafeRun()

        val result = cs.query(matQuery)(SimpleTableRow.simpleInstance.uuidColumn).runLog.unsafeRun

        result shouldBe Vector(1.1f :: HNil)

      }
    }
  }
}
