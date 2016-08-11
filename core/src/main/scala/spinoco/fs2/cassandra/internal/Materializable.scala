package spinoco.fs2.cassandra.internal

/**
  * Created by adamchlupacek on 10/08/16.
  *
  * A guard for creating materialized views only from tables
  */
sealed trait Materializable

sealed trait NotMaterializable

