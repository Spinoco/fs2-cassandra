package spinoco.fs2.cassandra.internal.ctype

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.`type`.codec.TypeCodecs
import scodec.bits.{BitVector, ByteVector}
import scodec.{Attempt, Codec, DecodeResult, Err, SizeBound}
import spinoco.fs2.cassandra.{CType, util}

//TODO: Unused, remove this?
object BooleanCType {

  val instance: CType[Boolean] = {
    val trueValue = ByteVector.apply[Byte](0x01).bits
    val falseValue = ByteVector.apply[Byte](0x00).bits

    new CType[Boolean] {
      def cqlType: DataType = TypeCodecs.BOOLEAN.getCqlType

      def cqlCodec(protocolVersion: ProtocolVersion): Codec[Boolean] = {
        new Codec[Boolean] {
          def encode(value: Boolean): Attempt[BitVector] =
            Attempt.successful(if (value) trueValue else falseValue)

          val sizeBound: SizeBound = SizeBound.exact(8)

          def decode(bits: BitVector): Attempt[DecodeResult[Boolean]] = {
            if (bits.isEmpty) Attempt.successful(DecodeResult(false, BitVector.empty))
            else if (bits.length < 8) Attempt.failure(Err.insufficientBits(8, bits.length))
            else {
              val byteValue = bits.take(8)
              if (byteValue == trueValue) Attempt.successful(DecodeResult(true, bits.drop(8)))
              else if (byteValue == falseValue) Attempt.successful(DecodeResult(false, bits.drop(8)))
              else Attempt.failure(Err(s"Expected boolean byte, got $byteValue instead"))
            }

          }
        }
      }


      def parse(cql: String): Attempt[Boolean] =
        util.attempt(TypeCodecs.BOOLEAN.parse(cql): Boolean)

      def format(a: Boolean): Attempt[String] =
        util.attempt(TypeCodecs.BOOLEAN.format(a))
    }
  }

}
