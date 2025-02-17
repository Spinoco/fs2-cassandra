package spinoco.fs2.cassandra


import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.metadata.schema._

import scala.collection.JavaConverters._

package object system {

  /** helper allowing to construct ALTER statement, that will modify keyspaced to `desired` state **/
  def migrateKeySpace(desired:KeySpace, maybeCurrent: Option[KeyspaceMetadata]):Seq[String] = {
    maybeCurrent match {
      case None => desired.cqlStatement
      case Some(current) =>
        if (desired.name.toLowerCase != current.getName.asInternal().toLowerCase) Nil
        else {
          val currentReplication = current.getReplication.asScala
          val desiredReplication = desired.strategyOptions.toMap + ("class" -> desired.strategyClass)

          val updateReplication = if (desiredReplication != currentReplication) {
            val m = desiredReplication.map { case (k,v) => s"'$k':'$v'" }.mkString("{",",","}")
            Seq(s"REPLICATION = $m")
          } else Nil

          val updateDurableWrites =
            if (desired.durableWrites == current.isDurableWrites) Nil
            else {
              Seq(s"DURABLE_WRITES = ${desired.durableWrites}")
            }

          if (updateReplication.isEmpty && updateDurableWrites.isEmpty) Nil
          else {
            val update = (updateReplication ++ updateDurableWrites).mkString(" AND ")
            Seq(s"ALTER KEYSPACE ${desired.name} WITH $update")
          }

        }

    }

  }

  /** checks whether these two columns are of the same name and type **/
  def sameColumnDef(nameA:String, tpeA:DataType)(nameB:String, tpeB:DataType):Boolean = {
    val typesEqual = tpeA.asCql(true, false) == tpeB.asCql(true, false)
    val namesEqual = nameA.equalsIgnoreCase(nameB)
    namesEqual && typesEqual
}

  /** checks whether the primary keys of given tables are the same **/
  def samePrimaryKey(current: TableMetadata, desired: AbstractTable[_,_,_,_]):Boolean = {
    val currentPk = current.getPartitionKey.asScala.map(_.getName.asInternal.toLowerCase).toSeq
    val desiredPk = desired.partitionKey.map(_.toLowerCase)
    val currentCk = current.getClusteringColumns.asScala.keys.map(_.getName.asInternal.toLowerCase).toSeq
    val desiredCk = desired.clusterKey.map(_.toLowerCase)
     desiredCk == currentCk && desiredPk == currentPk
  }

  /** migrates table to desired state comparing with current metadata of the table **/
  def migrateTable(desiredTable:Table[_,_,_,_], maybeCurrent:Option[TableMetadata]):Seq[String] = {
    maybeCurrent match {
      case None => desiredTable.cqlStatement
      case Some(current) =>
        val fullTableName = s"${desiredTable.keySpaceName}.${desiredTable.name}"

        if (desiredTable.name != current.getName.asInternal) Nil
        else if (!samePrimaryKey(current, desiredTable)) s"DROP TABLE $fullTableName" +: desiredTable.cqlStatement
        else {
          val currentColumns = current.getColumns.asScala.map { c => c._1.asInternal.toLowerCase -> c._2.getType }
          val desiredColumns =  desiredTable.columns

          val removed = currentColumns.filterNot { case (k,tpe) =>
            desiredColumns.exists { sameColumnDef(k,tpe) _ tupled }
          }
          val added = desiredColumns.filterNot { case (k, tpe) =>
            currentColumns.exists { sameColumnDef(k,tpe) _ tupled }
          }

          lazy val tableTemplate = s"ALTER TABLE $fullTableName"
          val cqlRemoved = removed.map { case (k, _) => s"$tableTemplate DROP $k" }
          val cqlAdded = added.map {case (k, tpe) => s"$tableTemplate ADD $k ${tpe.asCql(true, false)}"}
          val res = cqlRemoved ++ cqlAdded
          res.toSeq
        }
    }
  }
}
