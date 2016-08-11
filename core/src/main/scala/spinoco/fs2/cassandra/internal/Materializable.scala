package spinoco.fs2.cassandra.internal

import spinoco.fs2.cassandra.{AbstractTable, Table}

import scala.annotation.implicitNotFound

/**
  * Created by adamchlupacek on 10/08/16.
  *
  * A guard for creating materialized views only from tables
  */
@implicitNotFound("Cannot create a materialized view from query on materialized view")
trait Materializable[Q <: AbstractTable]

object Materializable{

  implicit def forTable[T <: Table[_, _, _, _]]: Materializable[T] =
    new Materializable[T] {}

}
