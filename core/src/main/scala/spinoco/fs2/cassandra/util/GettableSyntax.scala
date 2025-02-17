package spinoco.fs2.cassandra.util

import com.datastax.oss.driver.api.core.data.{GettableByIndex, GettableByName}
import scodec.bits.BitVector

import java.nio.ByteBuffer

/**
 * Please note that the returned BitVectors can be null.
 *
 * The codecs and cassandra use null and empty to denote different things.
 */
object GettableSyntax {
  /** Given a Gettable, get a single value (by index or key) as a BitVector or ByteVector (instead of ByteBuffer) */
  implicit class GettableByNameSyntax(val self: GettableByName) extends AnyVal {
    def getBitsByName(k: String): BitVector = {
      getBytesByName(self, k, BitVector(_), null.asInstanceOf[BitVector])
    }
  }

  implicit class GettableByIndexSyntax(val self: GettableByIndex) extends AnyVal {
    def getBitsByIndex(idx: Int)
    : BitVector = {
      getBytesByIndex(self, idx, BitVector(_), null.asInstanceOf[BitVector])
    }
  }
  private def getBytesByName[T](self: GettableByName, k: String, constructData: ByteBuffer => T, emptyData: T): T = {
    val bytesOrNull = self.getBytesUnsafe(k)
    if (bytesOrNull == null) {
      emptyData
    } else {
      constructData(bytesOrNull)
    }
  }

  private def getBytesByIndex[T](self: GettableByIndex, idx: Int, constructData: ByteBuffer => T, emptyData: T): T = {
    val bytesOrNull = self.getBytesUnsafe(idx)
    if (bytesOrNull == null) {
      emptyData
    } else {
      constructData(bytesOrNull)
    }
  }
}
