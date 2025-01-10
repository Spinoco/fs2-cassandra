package spinoco.fs2.cassandra.internal

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.data.{SettableByIndex, SettableByName}
import spinoco.fs2.cassandra.CType
import spinoco.fs2.cassandra.internal.CodecSerializer.CodecSerializeSyntax

import java.nio.ByteBuffer

object CodecWriter {
  implicit class CodecWriteSyntax[V](val tpe: CType[V]) extends AnyVal {

    def writeByNameSerialized[D <: SettableByName[D]](key: String, value: V, data: D, protocolVersion: ProtocolVersion): D = {
      tpe
        .serialize(value, protocolVersion)
        .toEither
        .left.map { e => throw new Throwable(e.message) }
        .right.map(bv => data.setBytesUnsafe(key, bv))
        .getOrElse(data)

    }

    def writeAtSerialized[D <: SettableByIndex[D]](idx: Int, value: V, data: D, protocolVersion: ProtocolVersion): D = {
      tpe
        .serialize(value, protocolVersion)
        .toEither
        .left.map { e => throw new Throwable(e.message) }
        .right.map(bv => data.setBytesUnsafe(idx, bv))
        .getOrElse(data)

    }

    def writeRawSerialized(key: String, value: V, protocolVersion: ProtocolVersion): Map[String, ByteBuffer] = {
      tpe
        .serialize(value, protocolVersion)
        .toEither
        .left.map { e => throw new Throwable(e.message) }
        .right.map(bv => Map(key -> bv))
        .getOrElse(Map.empty)
    }

    def writeFormatted(key: String, value: V): Map[String, String] = {
      tpe
        .format(value)
        .toEither
        .left.map { e => throw new Throwable(e.message) }
        .right.map(bv => Map(key -> bv))
        .getOrElse(Map.empty)
    }
  }
}
