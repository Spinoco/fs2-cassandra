package spinoco.fs2.cassandra.internal.ctype

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.internal.core.`type`.codec.FloatCodec
import scodec.bits.BitVector
import scodec.{Attempt, Codec, DecodeResult, SizeBound}
import spinoco.fs2.cassandra.{CType, util}

object FloatCType {

  val instance: CType[Float] = {
    val nativeCodec = new FloatCodec()
    new CType[Float] {
      def cqlType: DataType =
        nativeCodec.getCqlType

      def cqlCodec(protocolVersion: ProtocolVersion): Codec[Float] = {
        new Codec[Float] {
          def encode(value: Float): Attempt[BitVector] =
            scodec.codecs.float.encode(value)

          val sizeBound: SizeBound =
            SizeBound.choice(Seq(
              SizeBound.exact(0) // b/c empty is allowed too
              , scodec.codecs.float.sizeBound
            ))

          def decode(bits: BitVector): Attempt[DecodeResult[Float]] = {
            if (bits.isEmpty) Attempt.successful(DecodeResult(0f, BitVector.empty))
            else scodec.codecs.float.decode(bits)
          }
        }
      }

      def parse(cql: String): Attempt[Float] =
        util.attempt(nativeCodec.parse(cql) : Float)

      def format(a: Float): Attempt[String] =
        util.attempt(nativeCodec.format(a))
    }
  }

}
