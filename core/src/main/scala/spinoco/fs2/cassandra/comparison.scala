package spinoco.fs2.cassandra

object Comparison extends Enumeration {
  val EQ = Value("=")
  val GT = Value(">")
  val GTEQ = Value(">=")
  val LT = Value("<")
  val LTEQ = Value("<=")
}