---
layout: docs
title:  "Table Builder"
number: 4
---

# Table Builder

Given a `KeySpace` it is possible to define a `Table` by using the provided dsl.

### Example

Given a simple table definition (must be a `case class`):

```tut:book:silent
import java.util.UUID
import shapeless.tag._
import spinoco.fs2.cassandra.CType.Type1

case class SimpleTable (
  intColumn: Int,
  longColumn: Long,
  stringColumn: String,
  timeUuidColumn: UUID @@ Type1
)
```

We can define a `Table` with `partition`:

```tut:book:silent
import spinoco.fs2.cassandra.KeySpace

val ks = new KeySpace("test_ks")

val t1 = {
  ks.table[SimpleTable]
    .partition('intColumn)
    .build("test_table")
}
```

```tut:book
t1.cqlStatement
```

Another one with `partition` and `cluster`:

```tut:book:silent
val t2 = {
  ks.table[SimpleTable]
    .partition('intColumn)
    .cluster('longColumn)
    .build("test_table")
}
```

```tut:book
t2.cqlStatement
```

And another with `index`:

```tut:book:silent
val t3 = {
  ks.table[SimpleTable]
    .partition('intColumn)
    .indexBy('stringColumn, "str_idx")
    .build("test_table")
}
```

```tut:book
t3.cqlStatement
```
