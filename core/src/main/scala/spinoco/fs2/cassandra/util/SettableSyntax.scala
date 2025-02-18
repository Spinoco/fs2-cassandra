package spinoco.fs2.cassandra.util

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.data.{SettableByIndex, SettableByName}
import spinoco.fs2.cassandra.CType
import spinoco.fs2.cassandra.util.CodecSerializerSyntax.CodecSerializeDeserializeSyntax

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
          e => throw new Throwable(e.message),
          bv => data.setBytesUnsafe(key, bv)
        )
    }

    def writeAtSerialized[D <: SettableByIndex[D]](idx: Int, value: V, data: D, protocolVersion: ProtocolVersion): D = {
      tpe
        .serialize(value, protocolVersion)
        .toEither
        .fold(
          e => throw new Throwable(e.message),
          bv =>  data.setBytesUnsafe(idx, bv)
        )
    }

    def writeRawSerialized(key: String, value: V, protocolVersion: ProtocolVersion): Map[String, ByteBuffer] = {
      tpe
        .serialize(value, protocolVersion)
        .toEither
        .fold(
          e => throw new Throwable(e.message),
          bv => Map(key -> bv)
        )
    }

    def writeFormatted(key: String, value: V): Map[String, String] = {
      tpe
        .format(value)
        .toEither
        .fold(
          e => throw new Throwable(e.message),
          bv => Map(key -> bv)
        )
    }
  }
}
