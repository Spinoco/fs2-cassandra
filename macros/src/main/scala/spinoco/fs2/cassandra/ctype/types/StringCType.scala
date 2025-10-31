package spinoco.fs2.cassandra.ctype.types

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.internal.core.`type`.codec.StringCodec
import scodec.bits.BitVector
import scodec.{Attempt, Codec, DecodeResult, SizeBound}
import spinoco.fs2.cassandra.baseutil
import spinoco.fs2.cassandra.ctype.CType

import java.nio.charset.Charset

object StringCType {

  /**
    * Creates instance of CType for String
    * @param nativeCodec  Native CQl Codec for String
    * @param charset  Charset the string is encoded in
    */
  def instance(nativeCodec: StringCodec,  charset: Charset): CType[String] = {
    val stringCodec = scodec.codecs.string(charset)
    new CType[String] {
      def cqlType: DataType = nativeCodec.getCqlType

      def cqlCodec(protocolVersion: ProtocolVersion): Codec[String] = {
        new Codec[String] {
          def encode(value: String): Attempt[BitVector] =
            stringCodec.encode(value)

          def sizeBound: SizeBound =
            SizeBound.unknown

          def decode(bits: BitVector): Attempt[DecodeResult[String]] = {
            if (bits.isEmpty) Attempt.successful(DecodeResult("", bits))
            else stringCodec.decode(bits)
          }
        }
      }

      def parse(cql: String): Attempt[String] =
        baseutil.attempt(nativeCodec.parse(cql))

      def format(a: String): Attempt[String] =
        baseutil.attempt(nativeCodec.format(a))
    }
  }

}
