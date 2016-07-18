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
      case Some(clz) =>
        val withOptions =
          if (options.isEmpty) ""
          else options.map { case (k,v) => s"'$k': '$v'"}.mkString(" WITH OPTIONS = {",",","}")

        s"CREATE CUSTOM INDEX $name ON $ks.$table ($field) USING $clz$withOptions"

    }
  }
}


object IndexEntry {

  val SASIIndexClz = "org.apache.cassandra.index.sasi.SASIIndex"
}
