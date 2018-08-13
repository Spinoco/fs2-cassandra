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

### Example

Create the cluster once and pass it as a parameter to any components that might need it. Start a session for every transaction you want run (eg. insert statements).

```tut:book:silent
import cats.effect.IO
import com.datastax.driver.core.Cluster
import fs2.Stream
import spinoco.fs2.cassandra.CassandraCluster

val config = Cluster.builder().addContactPoints("127.0.0.1")

def doSomething(cluster: CassandraCluster[IO]): Stream[IO, Unit] = {
  cluster.session.evalMap(_ => IO.unit) // eg. execute a statement
}

CassandraCluster[IO](config).flatMap { cluster =>
  doSomething(cluster)
}
```
