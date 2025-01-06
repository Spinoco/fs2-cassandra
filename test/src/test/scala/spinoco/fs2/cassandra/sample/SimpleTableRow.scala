package spinoco.fs2.cassandra.sample

import java.net.InetAddress
import java.util.UUID

import fs2.Chunk
import shapeless.tag
import shapeless.tag._
import spinoco.fs2.cassandra.CType.{Ascii, Type1}

import scala.concurrent.duration.FiniteDuration


case class SimpleTableRow(
  intColumn: Int
  , longColumn: Long
  , stringColumn: String
  , asciiColumn: String @@ Ascii
  , floatColumn: Float
  , doubleColumn: Double
  , bigDecimalColumn: BigDecimal
  , bigIntColumn: BigInt
  , blobColumn: Chunk[Byte]
  , uuidColumn: UUID
  , timeUuidColumn: UUID @@ Type1
  , durationColumn: FiniteDuration
  , inetAddressColumn: InetAddress
  , enumColumn: TestEnumeration.Value
)

object SimpleTableRow {

  private val uuid = UUID.fromString("00000000-0000-0000-0000-000000000000")
  val simpleInstance = SimpleTableRow(
    intColumn = 1
    , longColumn = 2
    , stringColumn = "varchar string"
    , asciiColumn = tag[Ascii]("ascii string")
    , floatColumn =  1.1f
    , doubleColumn =  2.2d
    , bigDecimalColumn = BigDecimal(0.3d)
    , bigIntColumn = BigInt(3)
    , blobColumn = Chunk.bytes(Array.emptyByteArray)
    , uuidColumn = uuid
    , timeUuidColumn =  tag[Type1](uuid)
    , durationColumn = FiniteDuration(1,"s")
    , inetAddressColumn = InetAddress.getLocalHost
    , enumColumn = TestEnumeration.One
  )

}