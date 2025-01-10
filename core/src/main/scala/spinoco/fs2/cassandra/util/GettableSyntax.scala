package spinoco.fs2.cassandra.util

import com.datastax.oss.driver.api.core.data.{GettableByIndex, GettableByName}
import scodec.bits.{BitVector, ByteVector}
import shapeless.Typeable
import spinoco.fs2.cassandra.util.GettableSyntax.impl

import java.nio.ByteBuffer
import java.util.Optional

object GettableSyntax {
  implicit class GettableByNameSyntax(val self: GettableByName) extends AnyVal {
    def getBitsByName(k: String): BitVector = {
      impl.getBytesByName(self, k, BitVector(_), BitVector.empty)
    }
  }

  implicit class GettableByIndexSyntax(val self: GettableByIndex) extends AnyVal {
    def getBitsByIndex(idx: Int)
    : BitVector = {
      impl.getBytesByIndex(self, idx, BitVector(_), BitVector.empty)
    }
  }

  object impl {
    def getBytesByName[T](self: GettableByName, k: String, constructData: ByteBuffer => T, emptyData: T): T = {
      val bytesOrNull = self.getBytesUnsafe(k)
      if (bytesOrNull == null) {
        emptyData
      } else {
        constructData(bytesOrNull)
      }
    }

    def getBytesByIndex[T](self: GettableByIndex, idx: Int, constructData: ByteBuffer => T, emptyData: T): T = {
      val bytesOrNull = self.getBytesUnsafe(idx)
      if (bytesOrNull == null) {
        emptyData
      } else {
        constructData(bytesOrNull)
      }
    }
  }
}
