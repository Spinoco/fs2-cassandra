package spinoco.fs2.cassandra

import com.datastax.driver.core.{DataType, Row}
import shapeless.labelled.FieldType
import shapeless.ops.hlist.Prepend
import shapeless.ops.record.Selector
import shapeless.{::, HList, HNil, LabelledGeneric, Witness}
import spinoco.fs2.cassandra.builder._

sealed trait SchemaDDL {
  /** CQL Command representing this SchemaDDL object **/
  def cqlStatement:String
}


case class KeySpace(
  name:String
  , durableWrites:Boolean = true
  , strategyClass:String = "SimpleStrategy"
  , strategyOptions:Seq[(String,String)] = Seq("replication_factor" -> "1")
  , solr:Boolean = false
) extends SchemaDDL { self =>

  trait KsTblBuilder[A] {
    def partition[R <: HList,K](name:Witness.Aux[K])(
      implicit
      G:LabelledGeneric.Aux[A,R]
      , ev:Selector[R,K]
    ):TableBuilder[R, FieldType[K,ev.Out] :: HNil, HNil] = TableBuilder(self)
  }

  /** construct definition for table specified by type `A`. At least primary key must be specified **/
  def table[A]:KsTblBuilder[A] = new KsTblBuilder[A] {}

  /** constructs empty table definition **/
  def table:TableBuilder[HNil, HNil, HNil]= TableBuilder(self)


  lazy val cql = {
    val solrConfig = if (solr) Seq("Solr" -> "1") else Nil
    val replication = (("class" -> strategyClass) +: (strategyOptions ++ solrConfig)).map {case (k,v) => s"'$k':'$v'"}.mkString("{",",","}")

    s"CREATE KEYSPACE $name WITH REPLICATION = $replication AND DURABLE_WRITES = $durableWrites "
  }

  def cqlStatement: String = cql

  override def toString: String = s"KeySpace[$cql]"
}

object KeySpace {

  implicit class KeySpaceSyntax(val self: KeySpace) extends AnyVal {
    /** converts KeySpace to have SimpleStrategy. Useful for testing **/
    def asLocal:KeySpace = self.copy(
      strategyClass = "SimpleStrategy"
      , strategyOptions = Seq("replication_factor" -> "1")
    )

    /** enables DSE Solr on the keyspace **/
    def enableDSESolr:KeySpace = self.copy(solr = true)

  }

}






trait Table[R <: HList, PK <: HList, CK <: HList] extends SchemaDDL {

  /** creates definition of the query against the table **/
  def query:QueryBuilder[R,PK,CK, HNil, HNil]

  /** definition of delete to delete columns specified by `Q` and eventually returning `R0` as result **/
  def delete[Q,R0]:DeleteBuilder[R, PK, CK, PK, HNil]

  /** definition of update specified by columns and conditions and sets in `Q` returning `R` as result **/
  def update(implicit p:Prepend[PK,CK]):UpdateBuilder[R, PK, CK, p.Out,HNil]

  /** creates definition of the Insert DML statement against this table **/
  def insert(implicit p:Prepend[PK,CK]):InsertBuilder[R,p.Out]

  ///////////////////////////////////////

  /** name of the keyspace **/
  def keySpace:String
  /** name of the table **/
  def name:String
  /** any table specificc options **/
  def options:Map[String,String]
  /** all columns and their datatype **/
  def columns: Seq[(String, DataType)]
  /** partitioning key of primary key, possibly compound when more values. Guaranteed nonempty **/
  def partitionKey: Seq[String]
  /** cluster key(s), if any **/
  def clusterKey: Seq[String]

  /** reads row from the table, possibly converting it to `R` **/
  def read(row:Row):Either[Throwable,R]
}

