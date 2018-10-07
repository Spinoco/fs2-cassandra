---
layout: docs
title:  "Connection"
number: 1
---

# Establishing Connection

The `CassandraCluster` interface exposes a way to create a `CassandraSession` via its `session` method defined as follow:

```scala
def session: Stream[F, CassandraSession[F]]
```

It acquires a single session, that can be used to execute statements. Note that this emits only once, and session is closed when the resulting process terminates.

***NOTE: It is recommended to create a single `Session` and pass it as a parameter to any components that might need it.***

### Example

Given a `CassandraSession` execute a statement (eg. insert statements).

```tut:book:silent
import cats.effect.IO
import com.datastax.driver.core.Cluster
import fs2._
import fs2.Stream
import spinoco.fs2.cassandra.{CassandraCluster, CassandraSession}

import scala.concurrent.ExecutionContext

implicit val cxs : ContextShift[IO] = IO.contextShift(ExecutionContext.Implicits.global)

val config = Cluster.builder().addContactPoints("127.0.0.1")

def doSomething(session: CassandraSession[IO]) = {
  session.queryCql("SELECT * FROM ks.test") // or something else
}

val program: Stream[IO, Unit] =
  for {
    cluster <- Stream.resource(CassandraCluster.instance[IO](config, None))
    session <- Stream.resource(cluster.session)
    _       <- doSomething(session)
  } yield ()
```
