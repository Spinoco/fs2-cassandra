package spinoco.fs2.cassandra.util

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.data.{GettableByIndex, GettableByName}
import scodec.bits.BitVector
import shapeless.labelled.{FieldType, field}
import shapeless.{::, HList}
import spinoco.fs2.cassandra.CType
import spinoco.fs2.cassandra.util.CodecSerializerSyntax.CodecSerializeDeserializeSyntax

import java.nio.ByteBuffer

/**
 * Please note that the returned BitVectors can be null.
 *
 * The codecs and cassandra use null and empty to denote different things.
 */
object GettableSyntax {
  /** Given a Gettable, get a single value (by index or key) as a BitVector or ByteVector (instead of ByteBuffer)  */
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

  /** Given a bitvector, deserialize it, and take care of annotations and errors
   *
   * Typically used on the result of getBitsByName/Index
   * */
  implicit class BitVectorReadAsSyntax(val self: BitVector) extends AnyVal {
    def readAs[K,V,T <: HList](key: String, protocolVersion: ProtocolVersion)(tail: => Either[Throwable,T])( implicit tpe: CType[V]): Either[Throwable, ::[FieldType[K, V], T]] = {
      tpe.deserialize(self,protocolVersion).right.flatMap { read =>
          tail.right.map { tr =>
            field[K](read) :: tr
          }
        }
        .left.map(err => new Throwable(s"Failed to read $key from GettableByIndexData (protocol:$protocolVersion)", err))
        .left.map(AnnotatedException.withField(_, key))
    }
  }
}
