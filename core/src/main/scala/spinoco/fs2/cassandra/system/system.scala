package spinoco.fs2.cassandra

import com.datastax.driver.core.{DataType, KeyspaceMetadata, TableMetadata}

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

  /** migrates table to desired state comparing with current metadata of the table **/
  def migrateTable(desiredTable:Table[_,_,_,_], maybeCurrent:Option[TableMetadata]):Seq[String] = {
    // todo: support for index migration

    maybeCurrent match {
      case None => desiredTable.cqlStatement
      case Some(current) =>
        def samePrimaryKey:Boolean = {
          val currentPk = current.getPartitionKey.asScala.map(_.getName.toLowerCase)
          val currentCk = current.getClusteringColumns.asScala.map(_.getName.toLowerCase)
          desiredTable.clusterKey.map(_.toLowerCase) == currentCk && desiredTable.partitionKey.map(_.toLowerCase) == currentPk
        }

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
        val fullTableName = s"${desiredTable.keySpaceName}.${desiredTable.name}"


        if (desiredTable.name != current.getName) Nil
        else if (!samePrimaryKey) s"DROP TABLE $fullTableName" +: desiredTable.cqlStatement
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





}
