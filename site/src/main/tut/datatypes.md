---
layout: docs
title:  "Datatypes"
number: 2
---

# Supported Datatypes

`fs2-cassandra` makes use of `shapeless`'s tags to define custom types. Find below the current supported types:

#### Base Types

| Scala | Cassandra |
|-------|-----------|
| Int   | int       |
| Long  | bigint    |
| String | varchar  |
| String @@ Ascii | ascii |
| Float | float     |
| Double | double   |
| BigDecimal | decimal |
| BigInt  | varint  |
| Chunk[Byte] | blob |
| UUID | uuid       |
| UUID @@ Type1 | timeuuid |
| FiniteDuration | bigint |
| InetAddress | inet |
| Enumeration | varchar |

#### Collections and others

| Scala | Cassandra |
|-------|-----------|
| List | list |
| Vector | list |
| Seq | list |
| Set | set |
| Map[K, V] | map<k, v> |
| Tuple2[A, B] | frozen<tuple<a, b>> |
| Tuple3[A, B, C] | frozen<tuple<a, b, c>> |

### Example

```tut:book:silent
import java.net.InetAddress
import java.util.UUID

import fs2.Chunk
import shapeless.tag._
import spinoco.fs2.cassandra.CType.{Ascii, Type1}

import scala.concurrent.duration.FiniteDuration

object TestEnumeration extends Enumeration {
  val One, Two, Three = Value
}

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
```
