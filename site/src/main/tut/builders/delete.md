---
layout: docs
title:  "Delete Builder"
number: 8
---

# Delete Builder

The Delete builder DSL provides simple `row` delete and conditional methods `onlyIfExists` and `onlyIf` generating CQL statement for delete.

### Delete DSL

Given our [SimpleTable](./table) definition we will generate delete statement with the DSL.

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

#### Delete full row

Delete with `row` generate our basic CQL delete statement.

```tut:book:silent
val d1 = {
  t1.delete
    .row
    .build
}
```

```tut:book
d1.cqlStatement
```

Partition key is included by default by the builder when using `row`

##### Delete row with clustering key

To specify clustering key, simply use `cluster('columnName)`

```tut:book:silent
val d1c = {
  t2.delete
    .row
    .cluster('longColumn)
    .build
}
```

```tut:book
d1c.cqlStatement
```

#### Delete specific column 

CQL also allow to only delete specific columns for that `column` instead of `row` can be used.

First let's create a new table with an optional column fields, those fields have to be declared 
as `Option` since this will effectively put `null` as their values.

```tut:book:silent
case class SimpleTableOptional (
  intColumn: Int,
  longColumn: Long,
  stringColumn: Option[String],
  timeUuidColumn: Option[UUID @@ Type1]
)

val t3 = {
  ks.table[SimpleTableOptional]
    .partition('intColumn)
    .build("test_option_table")
}
```

Let's use `column('columnName)` to generate delete statement with only those optional columns for the row.

```tut:book:silent
val d2 = {
  t3.delete
    .column('stringColumn)
    .column('timeUuidColumn)
    .primary
    .build
}
```

```tut:book
d2.cqlStatement
```

#### Delete with conditionals

Cassandra allow doing conditionals delete which translate to `IF EXISTS` and `IF column = 'value'` in CQL.

##### Delete if exists

```tut:book:silent
val d1c = {
  t1.delete
    .row
    .onlyIfExists
    .build
}
```

```tut:book
d1c.cqlStatement
```

##### Delete only if condition

Cassandra CQL supports conditional delete with value checking, the DSL provide `onlyIf` builder for that use case. 

This example with check if `stringColumn` equals the string `cats` before deleting partition key 1.

`OnlyIf` requires 3 arguments;the column name, the value and a comparator provided by the Enumeration `Comparison`.

Let's first import the Comparison object

```tut:book:silent
import spinoco.fs2.cassandra.Comparison
```

Now we can build our `onlyIf` with `Comparison.EQ` since we want our column to equals the string `"cats"`.

```tut:book:silent
val d2c = {
  t1.delete
    .row
    .primary
    .onlyIf('stringColumn, "string_value", Comparison.EQ)
    .build
}
```

```tut:book
d2c.cqlStatement
```

##### Providing values from a Tuple with cqlFor

The previous builder will not fill the statement with the actual `"cats"` value nor the partition key;
in order to do so we need to use `cqlFor`.

```tut:book:silent
val d3c = { 
  // Starting from previous statement
  d2c.fromHList
     .fromTuple[(String, Int)]
     .cqlFor(("cats", 1))
}
```

Finally, we should see what is expected as CQL statement.

```tut:book
d3c
```

