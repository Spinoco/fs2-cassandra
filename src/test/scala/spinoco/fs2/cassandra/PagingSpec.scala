package spinoco.fs2.cassandra

import com.datastax.driver.core.PagingState
import fs2._
import fs2.util.Task
import spinoco.fs2.cassandra.sample.SimpleTableRow

trait PagingSpec extends SchemaSupport {



  s"Query paging (${cassandra.tag})" - {

    s"will page through results manually" in withSessionAndSimpleSchema { cs =>

      val fetchOneOptions = Options.defaultQuery.withFetchSize(1)

      def go(lastPage:Option[PagingState]):Stream[Task,SimpleTableRow] = {
        val o = lastPage.map(fetchOneOptions.startFrom).getOrElse(fetchOneOptions)
        cs.pageAll(strSelectAll,o).flatMap {
          case Right(str) => Stream.emit(str)
          case Left(Some(ps)) => go(Some(ps))
          case Left(None) => Stream.empty
        }
      }

      val result = go(None).runLog.unsafeRun


      result should have size(11*11)

    }

    s"will fetch up to fetchSize in single batch" in withSessionAndSimpleSchema { cs =>
      val fetchOneOptions = Options.defaultQuery.withFetchSize(1)

      def go(lastPage:Option[PagingState], cnt:Int):Stream[Task,Int] = {
        val o = lastPage.map(fetchOneOptions.startFrom).getOrElse(fetchOneOptions)
        cs.pageAll(strSelectAll,o).flatMap {
          case Right(str) => Stream.empty
          case Left(Some(ps)) => Stream.emit(cnt) ++ go(Some(ps), cnt + 1)
          case Left(None) => Stream.empty
        }
      }

      val result = go(None,0).runLog.unsafeRun

      result should have size(11*11)


    }

  }

}
