package spinoco.fs2.cassandra.baseutil

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.data.SettableByName
import spinoco.fs2.cassandra.baseutil.CodecSerializerSyntax.CodecSerializeDeserializeSyntax
import spinoco.fs2.cassandra.ctype.CType

import java.nio.ByteBuffer

/**
 * TODO: Revisit this: change the signatures to work with Attempt[D]
 */
object SettableSyntax {
  implicit class SettableWriteSyntax[V](val tpe: CType[V]) extends AnyVal {
    def writeByNameSerialized[D <: SettableByName[D]](key: String, value: V, data: D, protocolVersion: ProtocolVersion): D = {
      tpe
      .serialize(value, protocolVersion)
      .toEither
      .fold(
        e => throw new Throwable(e.message)
        , bv => data.setBytesUnsafe(key, bv)
      )
    }

    def writeRawSerialized(key: String, value: V, protocolVersion: ProtocolVersion): Map[String, ByteBuffer] = {
      tpe
      .serialize(value, protocolVersion)
      .toEither
      .fold(
        e => throw new Throwable(e.message)
        , bv => Map(key -> bv)
      )
    }

    def writeFormatted(key: String, value: V): Map[String, String] = {
      tpe
      .format(value)
      .toEither
      .fold(
        e => throw new Throwable(e.message)
        , bv => Map(key -> bv)
      )
    }
  }
}
