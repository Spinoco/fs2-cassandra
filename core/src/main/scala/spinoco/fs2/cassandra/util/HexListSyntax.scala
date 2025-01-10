package spinoco.fs2.cassandra.util

import scodec.bits.BitVector

object BitVectorPrinter {
  implicit class BitVectorPrinterSyntax(val self: BitVector) extends AnyVal {
    def toHexByteListString: String = {
      s"List(${self.bytes.toArray.map("0x%02X".format(_)).mkString(", ")})"
    }
  }
}
