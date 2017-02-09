package spinoco.fs2.cassandra.support

import com.datastax.driver.core.{Cluster, Session}
import com.datastax.driver.core.policies.ConstantReconnectionPolicy
import fs2.Stream._
import fs2.Task
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}
import spinoco.fs2.cassandra.{CassandraCluster, CassandraSession}

import scala.concurrent.SyncVar
import scala.sys.process.{Process, ProcessLogger}


/**
  * Created by pach on 08/06/16.
  */
trait DockerCassandra
  extends BeforeAndAfterAll
    with BeforeAndAfterEach { self: Suite =>
  import DockerCassandra._

  // override this to indicate whether containers shall be removed (true) once the test with C* is done.
  lazy val clearContainers:Boolean = true

  // override this if the C* container has to be started before invocation
  // when developing tests, this likely shall be false, so there is no additional overhead starting C*
  lazy val startContainers:Boolean = true

  // this has to be overridden to provide exact casandra definition. latest is default
  lazy val cassandra: CassandraDefinition = CassandraDefinition.latest


  // yields to true, if given KeySpace has to be preserved between tests, all other KeySpaces will be dropped after each test will end
  def preserveKeySpace(s:String):Boolean = systemKeySpaces.contains(s)

  // Port where CQL interface is available
  lazy val cqlPort: Int = 12000

  lazy val clusterConfig:Cluster.Builder =
    Cluster.builder()
      .addContactPoint(s"127.0.0.1")
      .withPort(cqlPort)
      .withReconnectionPolicy(new ConstantReconnectionPolicy(5000))



  private var dockerInstanceId:Option[String] = None
  private var clusterInstance:Option[Cluster] = None
   var sessionInstance:Option[(Session, CassandraSession[Task])] = None


  def withCluster(f: CassandraCluster[Task] => Any): Unit = {
    clusterInstance match {
      case None => throw new Throwable("Cassandra Cluster not ready")
      case Some(c) =>
        val ct = CassandraCluster.impl.create[Task](c).unsafeRun
        f(ct)
        ()
    }
  }

  def withSession(f: CassandraSession[Task] => Any):Unit = {
    sessionInstance match {
      case None => throw new Throwable("Cassandra session not yet ready")
      case Some((_,cs)) => f(cs); ()
    }
  }


  override protected def beforeAll(): Unit = {
    super.beforeAll()
    if (startContainers) {
      assertDockerAvailable
      downloadCImage(cassandra)
      dockerInstanceId = Some(startCassandra(cassandra, cqlPort))
    }
    val cluster = clusterConfig.build()
    clusterInstance = Some(cluster)
    val session = cluster.connect()
    val cs = CassandraSession.impl.mkSession[Task](session,cluster.getConfiguration.getProtocolOptions.getProtocolVersion).unsafeRun
    sessionInstance = Some(session -> cs)
  }


  override protected def afterAll(): Unit = {
    sessionInstance.foreach(_._1.close())
    clusterInstance.foreach(_.close())
    dockerInstanceId.foreach(stopCassandra(cassandra,_,clearContainers))
    super.afterAll()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    sessionInstance.foreach { case (_, cs) =>
      cleanupSchema(cs,cassandra)(preserveKeySpace)
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


  /** asserts that docker is available on host os **/
  def assertDockerAvailable:Unit = {
    val r = Process("docker -v").!!
    println(s"Verifying docker is available: $r")
  }

  def downloadCImage(cdef:CassandraDefinition):Unit = {
    val current:String= Process(s"docker images ${cdef.dockerImageUrl}").!!
    if (current.lines.size <= 1) {
      println(s"Pulling docker image for ${cdef.dockerImageUrl}")
      Process(s"docker pull ${cdef.dockerImageUrl}").!!
      ()
    }
  }

  /** cleans schema, leaving only system objects **/
  def cleanupSchema(cs:CassandraSession[Task], cassandra:CassandraDefinition)(preserveKeysSpace: String => Boolean):Unit = {
    cs.queryAll(cassandra.allKeySpaceQuery)
      .filter(n => !preserveKeysSpace(n))
      .flatMap { n =>
        eval(cs.executeCql(s"DROP KEYSPACE $n"))
      }
      .run.unsafeRun
  }


  def startCassandra(cdef:CassandraDefinition, cqlPort:Int):String= {

    val dockerId = new SyncVar[String]()
    val runCmd = s"docker run --name scalatest_cassandra_${System.currentTimeMillis()} -d -p $cqlPort:9042 ${cdef.dockerImageUrl}"

    val thread = new Thread(new Runnable {
      def run(): Unit = {
        val result = Process(runCmd).!!.trim
        var observer: Option[Process] = None
        val logger = ProcessLogger(
          { str =>
            if (str.contains("Starting listening for CQL clients on")) {
              observer.foreach(_.destroy())
              dockerId.put(result)
            }
          }, str => ()
        )

        println(s"Awaiting Cassandra startup (${cdef.dockerImageUrl} @ 127.0.0.1:$cqlPort)")
        val observeCmd = s"docker logs -f $result"
        observer = Some(Process(observeCmd).run(logger))

      }
    }, s"Cassandra ${cdef.dockerImageUrl} startup observer")
    thread.start()
    val id = dockerId.get
    println(s"Cassandra (${cdef.dockerImageUrl} @ 127.0.0.1:$cqlPort) started successfully as $id ")
    id
  }

  /**
    * Stops previously running container.
    *
    * @param cdef             definition of cassandra
    * @param instance         Id of docker instance to stop
    * @param clearContainer   When true, the container will be cleared (reclaimed)
    */
  def stopCassandra(cdef:CassandraDefinition, instance:String, clearContainer:Boolean):Unit = {
    if (clearContainer) {
      val killCmd = s"docker kill $instance"
      Process(killCmd).!!
      val rmCmd = s"docker rm $instance"
      Process(rmCmd).!!
      ()
    }
  }

}
