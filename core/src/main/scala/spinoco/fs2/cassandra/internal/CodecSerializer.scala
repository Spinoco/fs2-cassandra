package spinoco.fs2.cassandra.internal


import com.datastax.oss.driver.api.core.ProtocolVersion
import scodec.Attempt
import scodec.bits.{BitVector, ByteVector}
import spinoco.fs2.cassandra.CType

import java.nio.ByteBuffer

object CodecSerializer {
  implicit class CodecSerializeSyntax[V](val self: CType[V]) extends AnyVal {
    def serialize(v: V, protocolVersion: ProtocolVersion): Attempt[ByteBuffer] = {
       self
        .cqlCodec(protocolVersion)
        .encode(v)
        .map { bv => if (bv.isEmpty) null else bv.toByteBuffer }
    }

    def deserialize(bv: BitVector, protocolVersion: ProtocolVersion): Either[Throwable, V] = {
      self
        .cqlCodec(protocolVersion)
        .decode(bv)
        .toEither
        .left.map(e => new Throwable(e.message))
        .right.map(_.value)
    }

    def deserialize(bv: ByteBuffer, protocolVersion: ProtocolVersion): Either[Throwable, V] = {
      if (bv == null) {
        Left(new Throwable(s"Codec.deserialize: received null ByteBuffer. Codec: ${self.cqlCodec(protocolVersion)}, CqlType: ${self.cqlType}, asCql: ${self.cqlType.asCql(true, false)}"))
      } else {
        deserialize(BitVector(bv), protocolVersion)
      }
    }
  }


}
