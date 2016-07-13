package spinoco.fs2.cassandra.builder


case class IndexEntry(
  name:String
  , field:String
  , className:Option[String]
  , options:Map[String,String]
) {
  def cqlStatement(ks:String, table:String):String = {
    className match {
      case None => s"CREATE INDEX $name ON $ks.$table ($field)"
      case Some(clz) =>  s"CREATE CUSTOM INDEX $name ON $ks.$table ($field) USING $clz"

    }

  }
}
