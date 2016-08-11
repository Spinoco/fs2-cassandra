package spinoco.fs2.cassandra

import com.datastax.driver.core.DataType
import shapeless.labelled.FieldType
import shapeless.ops.hlist.Prepend
import shapeless.ops.record.Selector
import shapeless.{::, HList, HNil, LabelledGeneric, Witness}
import spinoco.fs2.cassandra.builder._

sealed trait SchemaDDL {
  /** CQL Command representing this SchemaDDL object **/
  def cqlStatement:Seq[String]
}

case class KeySpace(
  name:String
  , durableWrites:Boolean = true
  , strategyClass:String = "org.apache.cassandra.locator.SimpleStrategy"
  , strategyOptions:Seq[(String,String)] = Seq("replication_factor" -> "1")
) extends SchemaDDL { self =>

  trait KsTblBuilder[A] {
    def partition[R <: HList,K](name:Witness.Aux[K])(
      implicit
      G:LabelledGeneric.Aux[A,R]
      , ev:Selector[R,K]
    ):TableBuilder[R, FieldType[K,ev.Out] :: HNil, HNil, HNil] = TableBuilder(self, Nil, Seq(internal.keyOf(name)), Nil)
  }

  /** construct definition for table specified by type `A`. At least primary key must be specified **/
  def table[A]:KsTblBuilder[A] = new KsTblBuilder[A] {}

  /** constructs empty table definition **/
  def emptyTable:TableBuilder[HNil, HNil, HNil, HNil]= TableBuilder(self, Nil, Nil, Nil)


  lazy val cql = {
    val replication = (("class" -> strategyClass) +: strategyOptions).map {case (k,v) => s"'$k':'$v'"}.mkString("{",",","}")

    s"CREATE KEYSPACE $name WITH REPLICATION = $replication AND DURABLE_WRITES = $durableWrites "
  }

  def cqlStatement: Seq[String] = Seq(cql)

  override def toString: String = s"KeySpace[$cql]"
}

object KeySpace {

  implicit class KeySpaceSyntax(val self: KeySpace) extends AnyVal {
    /** converts KeySpace to have SimpleStrategy. Useful for testing **/
    def asLocal:KeySpace = self.copy(
      strategyClass = "SimpleStrategy"
      , strategyOptions = Seq("replication_factor" -> "1")
    )

    def withDurableWrites(durable:Boolean):KeySpace = {
      self.copy(durableWrites = durable)
    }

  }

}

sealed trait AbstractTable extends SchemaDDL {

  /** name of the keyspace **/
  def keySpaceName:String = keySpace.name
  /** reference to keyspace **/
  def keySpace: KeySpace
  /** full name of the table **/
  def fullName:String = s"$keySpaceName.$name"
  /** name of the table **/
  def name:String
  /** any table specific options **/
  def options:Map[String,String]
  /** all columns and their datatype **/
  def columns: Seq[(String, DataType)]
  /** partitioning key of primary key, possibly compound when more values. Guaranteed nonempty **/
  def partitionKey: Seq[String]
  /** cluster key(s), if any **/
  def clusterKey: Seq[String]

}

trait Table[R <: HList, PK <: HList, CK <: HList, IDX <: HList] extends AbstractTable {

  type Row = R
  type PartitionKey = PK
  type ClusterKay = CK

  /** definition of delete to delete columns specified by `Q` and eventually returning `R0` as result **/
  def delete[Q,R0]:DeleteBuilder[R, PK, CK, PK, HNil]

  /** definition of update specified by columns and conditions and sets in `Q` returning `R` as result **/
  def update(implicit p:Prepend[PK,CK]):UpdateBuilder[R, PK, CK, p.Out,HNil]

  /** creates definition of the Insert DML statement against this table **/
  def insert(implicit p:Prepend[PK,CK]):InsertBuilder[R,PK,CK,p.Out]

  ///////////////////////////////////////

  /** list of indexes on the table **/
  def indexes:Seq[IndexEntry]
}

object Table{

  implicit class TableSyntax[R <:HList, PK <: HList, CK <: HList, IDX <: HList](val self: Table[R, PK, CK, IDX]) extends AnyVal {

    /** Creates a query definition against this table **/
    def query: QueryBuilder[R, PK, CK, IDX, HNil, HNil, Table[R, PK, CK, IDX]] =
      QueryBuilder(self, Nil, Nil, Nil, Map.empty, None, allowFilteringFlag = false)

  }
}

trait MaterializedView[R <: HList, PK <: HList, CK <: HList] extends AbstractTable {

  /** This is the base table from which the view was created from **/
  def table: AbstractTable

}

object MaterializedView{

  implicit class MaterializedViewSyntax[R <:HList, PK <: HList, CK <: HList](val self: MaterializedView[R, PK, CK]) extends AnyVal {

    /** Creates a query definition against this view **/
    def query: QueryBuilder[R, PK, CK, HNil, HNil, HNil, MaterializedView[R, PK, CK]] =
      QueryBuilder(self, Nil, Nil, Nil, Map.empty, None, allowFilteringFlag = false)

  }
}
