---
layout: docs
title:  "Insert Builder"
number: 6
---

# Insert Builder

The Insert DSL provides methods such as `all`, `ifNotExists`, `withTTL` and `withTimestamp` which will generate the corresponding CQL statement.
It also allows partial column insertion with `column` and `columns`. Using `cqlFor` values can be provided to the actual statement in order to insert actual data.

### Insert DSL

Given our [SimpleTable](./table) definition we can insert data with different properties.

```tut:book:invisible
import java.util.UUID
import shapeless.tag._
import spinoco.fs2.cassandra.KeySpace
import spinoco.fs2.cassandra.CType.Type1

case class SimpleTable (
  intColumn: Int,
  longColumn: Long,
  stringColumn: String,
  timeUuidColumn: UUID @@ Type1
)

val ks = new KeySpace("test_ks")

val t1 = {
  ks.table[SimpleTable]
    .partition('intColumn)
    .build("test_table")
}

val t2 = {
  ks.table[SimpleTable]
    .partition('intColumn)
    .cluster('longColumn)
    .build("test_table")
}
```

#### All

All will generate standard CQL insert from Table row definition

```tut:book:silent
val i1 = {
  t1.insert
    .all
    .build
}
```

```tut:book
i1.cqlStatement
```

#### Insert if not exists

```tut:book:silent
val i2 = {
  t1.insert
    .all
    .ifNotExists
    .build
}
```

```tut:book
i2.cqlStatement
```

#### Insert with TTL

```tut:book:silent
val i3 = {
  t1.insert
    .all
    .withTTL('ttl)
    .build
}
```

```tut:book
i3.cqlStatement
```

#### Insert with only a subset of columns

```tut:book:silent
val i4 = {
  t1.insert
    .column('stringColumn)
    .build
}
```

```tut:book
i4.cqlStatement
```

Take note that since Table t1 has `intColumn` as partition key and is mandatory it is included.

#### Insert with subset of columns from a Case class

```tut:book:silent
import shapeless.LabelledGeneric

case class SimpleTableSubset(
  stringColumn: String,
  timeUuidColumn: UUID @@ Type1
)

val generic = LabelledGeneric[SimpleTableSubset]

val i5 = {
  t1.insert
    .columns[generic.Repr]  
    .build
}
```

Again here, we should see `intColumn` because it is the partition key.   

```tut:book
i5.cqlStatement
```

##### Providing values from a Tuple with cqlFor

The previous builders will not fill the statement with actual values; in order to do so we need to use `cqlFor` 

```tut:book:silent
val i6 = { 
  t1.insert
    .column('stringColumn)
    .build
    .fromHList
    .fromTuple[(String, Int)]
    .cqlFor(("Hello", 1))
}
```

```tut:book
i6
```

##### Providing a TTL value

```tut:book:silent
import shapeless._
import shapeless.syntax.singleton._
import spinoco.fs2.cassandra.CType.TTL

import scala.concurrent.duration._

val generic = LabelledGeneric[SimpleTable]

val i3ttl = i3.mapIn[(FiniteDuration, SimpleTable)] { case (dur, row) => ('ttl ->> tag[TTL](dur)) +: generic.to(row) }
```

```tut:book
i3ttl
```

