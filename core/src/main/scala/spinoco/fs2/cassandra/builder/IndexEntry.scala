package spinoco.fs2.cassandra.builder

case class IndexEntry(
  name: String
  , field: String
  , className: Option[String]
  , options: Map[String, String]
  , collectionTarget: Option[CollectionIndexTarget] = None
) {
  def cqlStatement(ks: String, table: String): String = {
    val fieldExpr = collectionTarget.fold(field)(_.wrap(field))
    className match {
      case None => s"CREATE INDEX $name ON $ks.$table ($fieldExpr)"
      case Some(clz) =>
        val withOptions =
          if (options.isEmpty) ""
          else options.map { case (k,v) => s"'$k': '$v'"}.mkString(" WITH OPTIONS = {",",","}")

        s"CREATE CUSTOM INDEX $name ON $ks.$table ($fieldExpr) USING '$clz'$withOptions"

    }
  }
}


object IndexEntry {

  val SASIIndexClz = "org.apache.cassandra.index.sasi.SASIIndex"
  val SAIIndexClz  = "org.apache.cassandra.index.sai.StorageAttachedIndex"
}
