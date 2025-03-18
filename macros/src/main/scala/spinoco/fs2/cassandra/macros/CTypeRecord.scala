package spinoco.fs2.cassandra.macros

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.data.{GettableByName, SettableByName}

import java.nio.ByteBuffer
import scala.language.experimental.macros

/**
 * CTypeRecord repressents a reader/writer class for a given HList type.
 */
trait CTypeRecord[H] {
  def types:Seq[(String,DataType)]
  def writeCql(r: H): Map[String, String]
  def writeRaw(r: H, protocolVersion: ProtocolVersion): Map[String, ByteBuffer]
  def writeByName[D <: SettableByName[D]](r: H, data: D, protocolVersion: ProtocolVersion): D
  def readByName(data: GettableByName, protocolVersion: ProtocolVersion): Either[Throwable, H]
  def readByNameIfExists(keys: Set[String], data: GettableByName, protocolVersion: ProtocolVersion): Either[Throwable, H]
}

object CTypeRecord {
  implicit def materialize[V]: CTypeRecord[V] = macro CTypeRecordGenerator.generate[V]
}

