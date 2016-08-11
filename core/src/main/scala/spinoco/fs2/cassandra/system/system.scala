package spinoco.fs2.cassandra

import com.datastax.driver.core._

import scala.collection.JavaConverters._

package object system {

  /** helper allowing to construct ALTER statement, that will modify keyspaced to `desired` state **/
  def migrateKeySpace(desired:KeySpace, maybeCurrent: Option[KeyspaceMetadata]):Seq[String] = {
    maybeCurrent match {
      case None => desired.cqlStatement
      case Some(current) =>
        if (desired.name.toLowerCase != current.getName.toLowerCase) Nil
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
    lazy val argsA = tpeA.getTypeArguments.asScala.map(_.getName)
    lazy val argsB = tpeB.getTypeArguments.asScala.map(_.getName)
    lazy val sameArgs:Boolean = {
      if (argsA.size != argsB.size) false
      else {
        (argsA zip argsB).forall{ case (nA,nB) => nA.isCompatibleWith(nB) }
      }
    }

    nameA.equalsIgnoreCase(nameB) &&
      tpeA.getName.isCompatibleWith(tpeB.getName) &&
      sameArgs
  }

  /** checks whether the primary keys of given tables are the same **/
  def samePrimaryKey(current: AbstractTableMetadata, desired: AbstractTable[_,_,_,_]):Boolean = {
    val currentPk = current.getPartitionKey.asScala.map(_.getName.toLowerCase)
    val currentCk = current.getClusteringColumns.asScala.map(_.getName.toLowerCase)
    desired.clusterKey.map(_.toLowerCase) == currentCk && desired.partitionKey.map(_.toLowerCase) == currentPk
  }

  /** migrates table to desired state comparing with current metadata of the table **/
  def migrateTable(desiredTable:Table[_,_,_,_], maybeCurrent:Option[TableMetadata]):Seq[String] = {
    // todo: support for index migration, AND options

    maybeCurrent match {
      case None => desiredTable.cqlStatement
      case Some(current) =>
        val fullTableName = s"${desiredTable.keySpaceName}.${desiredTable.name}"

        if (desiredTable.name != current.getName) Nil
        else if (!samePrimaryKey(current, desiredTable)) s"DROP TABLE $fullTableName" +: desiredTable.cqlStatement
        else {
          val currentColumns =
            current.getColumns.asScala.map { c => c.getName.toLowerCase -> c.getType }
          val desiredColumns =  desiredTable.columns

          val removed = currentColumns.filterNot { case (k,tpe) =>
            desiredColumns.exists { sameColumnDef(k,tpe) _ tupled }
          }
          val added = desiredColumns.filterNot { case (k, tpe) =>
            currentColumns.exists { sameColumnDef(k,tpe) _ tupled }
          }

          lazy val tableTemplate = s"ALTER TABLE $fullTableName"
          val cqlRemoved = removed.map { case (k, _) => s"$tableTemplate DROP $k" }
          val cqlAdded = added.map {case (k, tpe) => s"$tableTemplate ADD $k $tpe"}
          cqlRemoved ++ cqlAdded

        }
    }
  }

  /** migrates materialized view to desired state while comparing with current metadata of the table **/
  def migrateMaterializedView(desiredView: MaterializedView[_,_,_], maybeCurrent: Option[MaterializedViewMetadata]): Seq[String] = {
    //TODO support for options migration

    maybeCurrent match {
      case None => desiredView.cqlStatement
      case Some(current) =>

        def sameColumns: Boolean = {
          val currentColumns =
            current.getColumns.asScala.map { c => c.getName.toLowerCase -> c.getType }.sortBy(_._1)
          if(desiredView.columns.size != currentColumns.size) false
          else desiredView.columns.sortBy(_._1).zip(currentColumns).forall{case (dc, cc) => (sameColumnDef _).tupled(dc).tupled(cc)}
        }

        if (desiredView.name != current.getName) Nil
        else if (!samePrimaryKey(current.getBaseTable, desiredView.table)) desiredView.cqlStatement
        else if (!samePrimaryKey(current, desiredView) || !sameColumns) s"DROP MATERIALIZED VIEW ${desiredView.fullName}" +: desiredView.cqlStatement
        else Nil
    }
  }
}
