package spinoco.fs2.cassandra.internal


import com.datastax.oss.driver.api.core.ProtocolVersion
import scodec.Attempt
import scodec.bits.{BitVector, ByteVector}
import spinoco.KarelsTweaks.KotlinSyntax.KotlinSyntax
import spinoco.fs2.cassandra.CType
import spinoco.fs2.cassandra.util.BitVectorPrinter.BitVectorPrinterSyntax

import java.nio.ByteBuffer

object CodecSerializer {
  implicit class CodecSerializeSyntax[V](val self: CType[V]) extends AnyVal {
    def serialize(v: V, protocolVersion: ProtocolVersion): Attempt[ByteBuffer] = {
      val codec = self
        .cqlCodec(protocolVersion)

//      println(s"      Codec serializing: $codec, ${self.cqlType} }: $v ")

      codec
        .encode(v)
        .map { bv =>
//          println(s"                     ${bv.toHexByteListString}")
          if (bv.isEmpty) null else bv.toByteBuffer
        }
    }

    def deserialize(bv: BitVector, protocolVersion: ProtocolVersion): Either[Throwable, V] = {
      val codec = self
        .cqlCodec(protocolVersion)

//      print(s"      Decodec: $codec, ${self.cqlType} ${self.cqlType.asCql(true, false)}: $bv}")
//      println(s" ---> ${bv.toHexByteListString}")

      codec
        .decode(bv)
        .toEither
        .left.map(e => new Throwable(e.message))
        .right.map(
          _
            .value
//            .also{ res => println(s"        => $res")}
        )
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
