package spinoco.fs2.cassandra

import shapeless.Witness

/**
  * Created by pach on 04/06/16.
  */
package object internal {

  def keyOf[A](implicit wt:Witness.Aux[A]):String = asKeyName(wt.value)

  def asKeyName(a:Any):String = {
    a match {
      case sym:Symbol => sym.name
      case other => other.toString
    }
  }


}
