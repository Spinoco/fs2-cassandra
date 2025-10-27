package spinoco.fs2.cassandra.baseutil

import com.datastax.oss.driver.api.core.ProtocolVersion
import scodec.Attempt
import scodec.bits.BitVector
import spinoco.fs2.cassandra.ctype.CType

import java.nio.ByteBuffer

object CodecSerializerSyntax {
  implicit class CodecSerializeDeserializeSyntax[V](val self: CType[V]) extends AnyVal {
    def serialize(v: V, protocolVersion: ProtocolVersion): Attempt[ByteBuffer] = {
      self
      .cqlCodec(protocolVersion)
      .encode(v)
      .map { bv => if (bv != null) bv.toByteBuffer else null }
    }

    def deserialize(bv: BitVector, protocolVersion: ProtocolVersion): Either[Throwable, V] = {
      self
      .cqlCodec(protocolVersion)
      .decode(bv)
      .toEither
      .left.map(e => new Throwable(e.message))
      .map(_.value)
    }

    def deserialize(bv: ByteBuffer, protocolVersion: ProtocolVersion): Either[Throwable, V] = {
      if (bv == null) {
        deserialize(null.asInstanceOf[BitVector], protocolVersion)
          .left.map ( t => new Throwable( s"Codec.deserialize: received null ByteBuffer. Codec: ${self.cqlCodec(protocolVersion)}, CqlType: ${self.cqlType}, asCql: ${self.cqlType.asCql(true, false)}", t ) )
      } else {
        deserialize(BitVector(bv), protocolVersion)
      }
    }
  }
}
