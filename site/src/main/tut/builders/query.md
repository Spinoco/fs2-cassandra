---
layout: docs
title:  "Query Builder"
number: 5
---

# Query Builder

The Query DSL provides methods such as `all`, `column`, `partition` and `cluster` which will generate the correspoding CQL statement.

### Query DSL

Given our [SimpleTable](./table) definition we can create different queries.

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

```tut:book:silent
val q1 = t1.query.all.build
```

```tut:book
q1.cqlStatement
```

#### Column

```tut:book:silent
val q2 = t1.query.column('stringColumn).build
```

```tut:book
q2.cqlStatement
```

#### Column with alias

```tut:book:silent
val q3 = t1.query.columnAs('stringColumn, "my_alias").build
```

```tut:book
q3.cqlStatement
```

#### Function: Count

```tut:book:silent
import spinoco.fs2.cassandra.functions

val q4 = t1.query.function(functions.count, "rows_count").build
```

```tut:book
q4.cqlStatement
```

#### Function: DateOf

```tut:book:silent
val q5 = t1.query.functionAt(functions.dateOf, 'timeUuidColumn, "time_of_uuid").build
```

```tut:book
q5.cqlStatement
```

#### Function: UnixTimestampOf

```tut:book:silent
val q6 = t1.query.functionAt(functions.unixTimestampOf, 'timeUuidColumn, "timestamp_of_uuid").build
```

```tut:book
q6.cqlStatement
```

#### Function: WriteTimeOf

```tut:book:silent
val q7 = t1.query.functionAt(functions.writeTimeOfMicro[String], 'stringColumn, "write_time_of").build
```

```tut:book
q7.cqlStatement
```

#### Function: TTLOf

```tut:book:silent
val q8 = t1.query.functionAt(functions.ttlOf[String], 'stringColumn, "ttl_of").build
```

```tut:book
q8.cqlStatement
```

#### Partition

```tut:book:silent
val q9 = t1.query.column('stringColumn).partition.build
```

```tut:book
q9.cqlStatement
```

#### Partition and Cluster

```tut:book:silent
import spinoco.fs2.cassandra.Comparison

val q10 = t2.query.all.partition.cluster('longColumn, Comparison.GTEQ).build
```

```tut:book
q10.cqlStatement
```

#### Limit

```tut:book:silent
val q11 = t1.query.all.limit(1).build
```

```tut:book
q11.cqlStatement
```

#### Allow Filtering

```tut:book:silent
val q12 = t1.query.all.allowFiltering.build
```

```tut:book
q12.cqlStatement
```

#### Order By

```tut:book:silent
val q13 = t2.query.all.orderBy('longColumn, ascending = true).build
```

```tut:book
q13.cqlStatement
```


