package spinoco.fs2.cassandra.internal.ctype

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.internal.core.`type`.codec.BigIntCodec
import scodec.bits.BitVector
import scodec.{Attempt, Codec, DecodeResult, SizeBound}
import spinoco.fs2.cassandra.{CType, util}

object BigIntCType {

  def instance(cqlDataType: DataType): CType[Long] = {
    val nativeCodec = new BigIntCodec()
    new CType[Long] {
      def cqlType: DataType = cqlDataType

      def cqlCodec(protocolVersion: ProtocolVersion): Codec[Long] = {
        new Codec[Long] {
          def encode(value: Long): Attempt[BitVector] =
            scodec.codecs.int64.encode(value)

          val sizeBound: SizeBound =
            SizeBound.choice(Seq(
              SizeBound.exact(0) // b/c empty is allowed too
              , scodec.codecs.float.sizeBound
            ))

          def decode(bits: BitVector): Attempt[DecodeResult[Long]] = {
            if (bits.isEmpty) Attempt.successful(DecodeResult(0, BitVector.empty))
            else scodec.codecs.int64.decode(bits)
          }
        }
      }

      def parse(cql: String): Attempt[Long] =
        util.attempt(nativeCodec.parse(cql) : Long)

      def format(a: Long): Attempt[String] =
        util.attempt(nativeCodec.format(a))
    }
  }

}
