package spinoco.fs2.cassandra

import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.metadata.schema._
import spinoco.fs2.cassandra.builder.IndexEntry

import scala.jdk.CollectionConverters._

package object system {

  /** helper allowing to construct ALTER statement, that will modify keyspaced to `desired` state **/
  def migrateKeySpace(desired: KeySpace, maybeCurrent: Option[KeyspaceMetadata]): Seq[String] = {
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
  def sameColumnDef(nameA: String, tpeA: DataType)(nameB: String, tpeB: DataType): Boolean = {
    val typesEqual = tpeA.asCql(true, false) == tpeB.asCql(true, false)
    val namesEqual = nameA.equalsIgnoreCase(nameB)
    namesEqual && typesEqual
  }

  /** checks whether the primary keys of given tables are the same **/
  def samePrimaryKey(current: TableMetadata, desired: AbstractTable[_, _, _, _]): Boolean = {
    val currentPk = current.getPartitionKey.asScala.map(_.getName.asInternal.toLowerCase).toSeq
    val desiredPk = desired.partitionKey.map(_.toLowerCase)
    val currentCk = current.getClusteringColumns.asScala.keys.map(_.getName.asInternal.toLowerCase).toSeq
    val desiredCk = desired.clusterKey.map(_.toLowerCase)
    desiredCk == currentCk && desiredPk == currentPk
  }

  /** migrates table to desired state comparing with current metadata of the table **/
  def migrateTable(desiredTable: Table[_, _, _, _], maybeCurrent: Option[TableMetadata]): Seq[String] = {
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

          val indexStatements = migrateIndexes(desiredTable, current)

          val res = cqlRemoved ++ cqlAdded ++ indexStatements
          res.toSeq
        }
    }
  }

  /** compares desired indexes against current indexes, returns DROP/CREATE statements **/
  def migrateIndexes(desiredTable: Table[_, _, _, _], current: TableMetadata): Seq[String] = {
    val currentIndexes = current.getIndexes.asScala
    val desiredIndexes = desiredTable.indexes

    val desiredByName = desiredIndexes.map(idx => idx.name.toLowerCase -> idx).toMap

    // indexes to drop: exist in current but not in desired, or exist but changed
    val toDrop = currentIndexes.flatMap { case (cqlId, meta) =>
      val name = cqlId.asInternal.toLowerCase
      desiredByName.get(name) match {
        case None =>
          // index exists in C* but not desired - drop it
          Some(s"DROP INDEX ${desiredTable.keySpaceName}.$name")
        case Some(desired) =>
          // index exists in both - check if it changed
          if (!sameIndex(desired, meta, desiredTable.keySpaceName, desiredTable.name)) {
            Some(s"DROP INDEX ${desiredTable.keySpaceName}.$name")
          } else None
      }
    }

    // indexes to create: not in current, or were dropped because they changed
    val droppedNames = toDrop.map(_.split('.').last.trim.toLowerCase).toSet
    val currentNames = currentIndexes.keys.map(_.asInternal.toLowerCase).toSet

    val toCreate = desiredIndexes.flatMap { desired =>
      val name = desired.name.toLowerCase
      if (!currentNames.contains(name) || droppedNames.contains(name)) {
        Some(desired.cqlStatement(desiredTable.keySpaceName, desiredTable.name))
      } else None
    }

    (toDrop ++ toCreate).toSeq
  }

  /** checks whether a desired IndexEntry matches the current IndexMetadata **/
  def sameIndex(desired: IndexEntry, current: IndexMetadata, ks: String, table: String): Boolean = {
    val currentOptions = current.getOptions.asScala

    // compare class name
    val classMatches = desired.className match {
      case None => current.getKind != IndexKind.CUSTOM
      case Some(clz) => currentOptions.get("class_name").contains(clz)
    }

    // compare target column
    val targetMatches = currentOptions.get("target").exists { target =>
      val desiredField = desired.collectionTarget.fold(desired.field)(_.wrap(desired.field))
      target.equalsIgnoreCase(desiredField)
    }

    // compare options (exclude internal keys like class_name and target)
    val internalKeys = Set("class_name", "target")
    val currentUserOptions = currentOptions.filterNot { case (k, _) => internalKeys.contains(k) }
    val optionsMatch = desired.options == currentUserOptions

    classMatches && targetMatches && optionsMatch
  }
}
