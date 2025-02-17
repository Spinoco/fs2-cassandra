package spinoco.fs2.cassandra.util

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.data.{GettableByIndex, GettableByName}
import scodec.bits.BitVector
import shapeless.labelled.{FieldType, field}
import spinoco.fs2.cassandra.CType
import spinoco.fs2.cassandra.internal.CodecSerializer.CodecSerializeSyntax

import java.nio.ByteBuffer
import shapeless.{::, HList, HNil, Witness}

object CTypeReader {
  implicit class CTypeReaderSyntax(val self: BitVector)
    extends AnyVal {

    /** Given a bitvector, deserialize it, and take care of annotations and errors */
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
