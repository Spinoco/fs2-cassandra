package spinoco.fs2.cassandra.support

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.datastax.oss.driver.api.core.config.{DefaultDriverOption, DriverConfigLoader}
import com.datastax.oss.driver.api.core.{CqlSession, CqlSessionBuilder}
import fs2.Stream._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}
import spinoco.fs2.cassandra.CassandraSession

import java.net.InetSocketAddress
import java.time.Duration



/**
  * Created by pach on 08/06/16.
  */
trait DockerCassandra
  extends BeforeAndAfterAll
    with BeforeAndAfterEach { self: Suite =>
  import DockerCassandra._


  // Cassandra version for display purposes, controlled via CASSANDRA_SPEC_VERSION environment variable
  lazy val cassandraVersion: String = sys.env.getOrElse("CASSANDRA_SPEC_VERSION", "3.11")


  // yields to true, if given KeySpace has to be preserved between tests, all other KeySpaces will be dropped after each test will end
  def preserveKeySpace(s:String):Boolean = systemKeySpaces.contains(s)

  // Port where CQL interface is available
  lazy val cqlPort: Int = 12000

  def clusterConfig: CqlSessionBuilder = {
    val loader = DriverConfigLoader
      .programmaticBuilder
      .withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, Duration.ofMillis(20000))
      .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(20000))
      .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofMillis(20000))
      .withDuration(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT, Duration.ofMillis(20000))
      .withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofMillis(20000))
      .build

    CqlSession.builder()
      .withConfigLoader(loader)
      .addContactPoint(InetSocketAddress.createUnresolved(s"127.0.0.1", cqlPort))
      .withLocalDatacenter("datacenter1")
  }


  var sessionInstance:Option[(CqlSession, CassandraSession[IO])] = None


  def withSession(f: CassandraSession[IO] => Any):Unit = {
    sessionInstance match {
      case None => throw new Throwable("Cassandra session not yet ready")
      case Some((_,cs)) => f(cs); ()
    }
  }


  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // Assume Cassandra is already running (started externally via scripts/start-cassandra.sh)
    println(s"Connecting to Cassandra $cassandraVersion at 127.0.0.1:$cqlPort")
    val session = clusterConfig.build()
    val cs = CassandraSession.impl.mkSession[IO](session, session.getContext.getProtocolVersion).unsafeRunSync()
    sessionInstance = Some(session -> cs)
  }


  override protected def afterAll(): Unit = {
    sessionInstance.foreach(_._1.close())
    // NOTE: Container cleanup is handled externally via scripts/stop-cassandra.sh
    super.afterAll()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    sessionInstance.foreach { case (_, cs) =>
      cleanupSchema(cs)(preserveKeySpace)
    }
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
  }
}

object DockerCassandra {

  val systemKeySpaces:Set[String]=  Set(
    "system_auth", "system_schema", "system_distributed", "system", "system_traces"
  )

  /** cleans schema, leaving only system objects **/
  def cleanupSchema(cs:CassandraSession[IO])(preserveKeysSpace: String => Boolean):Unit = {
    import spinoco.fs2.cassandra.system.schema
    cs.queryAll(schema.queryAllKeySpaces.map(_.keyspace_name))
      .filter(n => !preserveKeysSpace(n))
      .flatMap { n => eval{ cs.executeCql(s"DROP KEYSPACE $n") } }
      .compile.drain.unsafeRunSync()
  }


}
